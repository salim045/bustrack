import json
import os
import psycopg2
from math import radians, sin, cos, sqrt, atan2
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC_IN     = os.getenv("KAFKA_TOPIC_RAW", "gps-raw")
TOPIC_OUT    = os.getenv("KAFKA_TOPIC_ENRICHED", "gps-enriched")

def get_db():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME", "busdb"),
        user=os.getenv("DB_USER", "bususer"),
        password=os.getenv("DB_PASSWORD", "buspass")
    )

def load_stations():
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("SELECT id, name, lat, lng, radius_km FROM stations ORDER BY id")
        rows = cur.fetchall()
        conn.close()
        stations = [{"id":r[0],"name":r[1],"lat":r[2],"lng":r[3],"radius_km":r[4]} for r in rows]
        print(f"[ETL] {len(stations)} stations chargees depuis PostgreSQL")
        return stations
    except Exception as e:
        print(f"[ETL] Erreur chargement stations: {e}")
        return []

def haversine(lat1, lng1, lat2, lng2):
    R = 6371
    dlat = radians(lat2 - lat1)
    dlng = radians(lng2 - lng1)
    a = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlng/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1-a))

def detect_segment(lat, lng, stations):
    closest_station = None
    closest_dist    = float("inf")
    for s in stations:
        dist = haversine(lat, lng, s["lat"], s["lng"])
        if dist < closest_dist:
            closest_dist    = dist
            closest_station = s
    if closest_station and closest_dist <= closest_station["radius_km"]:
        return closest_station["id"], closest_station["name"], round(closest_dist, 4)
    if closest_station:
        return closest_station["id"], f"vers_{closest_station['name']}", round(closest_dist, 4)
    return 0, "unknown", 0

consumer = KafkaConsumer(
    TOPIC_IN,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="etl-group-5",
    auto_offset_reset="latest"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

prev_positions = {}
reload_counter = 0
print(f"[ETL] Demarre — ecoute sur {TOPIC_IN}")
stations = load_stations()

for message in consumer:
    raw = message.value
    try:
        reload_counter += 1
        if reload_counter >= 20:
            stations       = load_stations()
            reload_counter = 0
        bus_id = raw.get("bus_id", "BUS_001")
        lat    = float(raw["lat"])
        lng    = float(raw["lng"])
        speed  = float(raw.get("speed_kmh", 0))
        hour   = int(raw.get("hour", datetime.now().hour))
        alt    = float(raw.get("altitude", 0))
        segment_id, bus_stop, dist_to_stop = detect_segment(lat, lng, stations)
        prev   = prev_positions.get(bus_id, (lat, lng))
        length = haversine(prev[0], prev[1], lat, lng)
        prev_positions[bus_id] = (lat, lng)
        enriched = {
            "bus_id":       bus_id,
            "segment":      segment_id,
            "bus_stop":     bus_stop,
            "lat":          lat,
            "lng":          lng,
            "altitude":     alt,
            "speed_kmh":    speed,
            "length":       round(length, 4),
            "dist_to_stop": dist_to_stop,
            "hour":         hour,
            "timestamp":    raw.get("timestamp", datetime.now().isoformat())
        }
        producer.send(TOPIC_OUT, value=enriched)
        print(f"[ETL] bus={bus_id} | stop={bus_stop} | dist={dist_to_stop:.3f}km | speed={speed} km/h")
    except Exception as e:
        print(f"[ETL] ERREUR: {e}")
