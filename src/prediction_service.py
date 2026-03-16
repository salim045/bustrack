import json
import os
import pickle
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer

load_dotenv(dotenv_path="/mnt/c/users/dell/documents/bus-prediction/.env")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC_IN     = os.getenv("KAFKA_TOPIC_ENRICHED", "gps-enriched")
TOPIC_OUT    = os.getenv("KAFKA_TOPIC_PREDICTIONS", "predictions")
MODEL_DIR    = "/mnt/c/users/dell/documents/bus-prediction/models"

with open(f"{MODEL_DIR}/model_run.pkl",   "rb") as f:
    model_run = pickle.load(f)
with open(f"{MODEL_DIR}/model_dwell.pkl", "rb") as f:
    model_dwell = pickle.load(f)

print("[PRED] Modeles charges")

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST",     "localhost"),
        port=os.getenv("DB_PORT",     5432),
        dbname=os.getenv("DB_NAME",   "busdb"),
        user=os.getenv("DB_USER",     "bususer"),
        password=os.getenv("DB_PASSWORD", "buspass")
    )

conn   = get_connection()
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS predictions (
        id               SERIAL PRIMARY KEY,
        bus_id           VARCHAR(50),
        segment          INT,
        bus_stop         VARCHAR(100),
        speed_kmh        FLOAT,
        hour             INT,
        lat              FLOAT,
        lng              FLOAT,
        altitude         FLOAT,
        run_time_pred    FLOAT,
        dwell_time_pred  FLOAT,
        eta_seconds      INT,
        arrival_datetime TIMESTAMP,
        created_at       TIMESTAMP DEFAULT NOW()
    )
""")
conn.commit()
print("[PRED] Table PostgreSQL prete")

consumer = KafkaConsumer(
    TOPIC_IN,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="prediction-group-3",
    auto_offset_reset="latest"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print(f"[PRED] Demarre — ecoute sur {TOPIC_IN}")

for message in consumer:
    data = message.value
    try:
        bus_id   = data["bus_id"]
        speed    = data["speed_kmh"]
        length   = data.get("length", 1.0)
        hour     = data["hour"]
        segment  = data["segment"]
        bus_stop = data["bus_stop"]
        lat      = data.get("lat", 0)
        lng      = data.get("lng", 0)
        altitude = data.get("altitude", 0)

        X_run = pd.DataFrame(
            [[speed, length, hour]],
            columns=["speed_kmh", "length", "hour"]
        )
        run_time_pred = float(model_run.predict(X_run)[0])

        X_raw = pd.DataFrame(
            [[bus_stop, hour, segment]],
            columns=["bus_stop", "hour", "segment"]
        )
        X_dwell = pd.get_dummies(X_raw, columns=["bus_stop"])
        model_cols = model_dwell.get_booster().feature_names
        for col in model_cols:
            if col not in X_dwell.columns:
                X_dwell[col] = 0
        X_dwell = X_dwell[model_cols]
        dwell_time_pred = float(model_dwell.predict(X_dwell)[0])

        eta_seconds = int(round(run_time_pred + dwell_time_pred))
        arrival_dt  = datetime.now() + timedelta(seconds=eta_seconds)

        result = {
            "bus_id":           bus_id,
            "segment":          segment,
            "bus_stop":         bus_stop,
            "lat":              lat,
            "lng":              lng,
            "altitude":         altitude,
            "speed_kmh":        speed,
            "run_time_pred":    round(run_time_pred, 2),
            "dwell_time_pred":  round(dwell_time_pred, 2),
            "eta_seconds":      eta_seconds,
            "arrival_datetime": arrival_dt.isoformat()
        }

        producer.send(TOPIC_OUT, value=result)

        cursor.execute("""
            INSERT INTO predictions
            (bus_id, segment, bus_stop, speed_kmh, hour,
             lat, lng, altitude,
             run_time_pred, dwell_time_pred,
             eta_seconds, arrival_datetime)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (bus_id, segment, bus_stop, speed, hour,
              lat, lng, altitude,
              run_time_pred, dwell_time_pred,
              eta_seconds, arrival_dt))
        conn.commit()

        print(f"[PRED] bus={bus_id} | lat={lat:.6f} | lng={lng:.6f} "
              f"| speed={speed} | ETA={eta_seconds}s "
              f"| arrivee={arrival_dt.strftime('%H:%M:%S')}")

    except Exception as e:
        print(f"[PRED] ERREUR: {e}")
        conn.rollback()
