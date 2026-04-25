import os
import pickle
import pandas as pd
import psycopg2
from math import radians, sin, cos, sqrt, atan2
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Header
from typing import Optional
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

MODEL_DIR = os.getenv("MODEL_DIR", "/app/models")
with open(f"{MODEL_DIR}/model_run.pkl",   "rb") as f:
    model_run = pickle.load(f)
with open(f"{MODEL_DIR}/model_dwell.pkl", "rb") as f:
    model_dwell = pickle.load(f)
print("[API] Modeles charges")

app = FastAPI(title="Bus Prediction API", version="1.0.0")
app.add_middleware(CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

class StationIn(BaseModel):
    name:      str
    lat:       float
    lng:       float
    radius_km: float = 0.3

class BusIn(BaseModel):
    id:   str
    name: str

def get_user(username, password):
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT username, password, role FROM users WHERE username=%s AND password=%s",
                (username, password))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return {"username": row[0], "password": row[1], "role": row[2]}

def check_admin(x_user: str = None, x_password: str = None):
    if not x_user or not x_password:
        raise HTTPException(status_code=401, detail="Authentification requise")
    user = get_user(x_user, x_password)
    if not user:
        raise HTTPException(status_code=401, detail="Identifiants incorrects")
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Acces refuse — admin seulement")
    return True

def get_db():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME", "busdb"),
        user=os.getenv("DB_USER", "bususer"),
        password=os.getenv("DB_PASSWORD", "buspass")
    )

def haversine(lat1, lng1, lat2, lng2):
    R = 6371
    dlat = radians(lat2 - lat1)
    dlng = radians(lng2 - lng1)
    a = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlng/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1-a))

def predict_run_time_for_segment(length_km, speed_kmh, hour):
    X = pd.DataFrame([[speed_kmh, length_km, hour]],
                     columns=["speed_kmh","length","hour"])
    return max(0, float(model_run.predict(X)[0]))

def predict_dwell_time_for_stop(stop_name, hour, segment):
    X_raw = pd.DataFrame([[stop_name, hour, segment]],
                         columns=["bus_stop","hour","segment"])
    X = pd.get_dummies(X_raw, columns=["bus_stop"])
    model_cols = model_dwell.get_booster().feature_names
    for col in model_cols:
        if col not in X.columns:
            X[col] = 0
    X = X[model_cols]
    return max(0, float(model_dwell.predict(X)[0]))

def split_distance_into_segments(total_km, segment_size_km=1.5):
    if total_km <= segment_size_km:
        return [total_km]
    segments = []
    remaining = total_km
    while remaining > 0:
        seg = min(segment_size_km, remaining)
        segments.append(round(seg, 4))
        remaining = round(remaining - seg, 4)
    return segments

def interpolate_points(lat1, lng1, lat2, lng2, n_points):
    points = []
    for i in range(n_points + 1):
        t = i / n_points
        points.append((lat1 + t*(lat2-lat1), lng1 + t*(lng2-lng1)))
    return points

def get_real_speed(bus_id, speed_raw):
    if speed_raw and speed_raw > 0:
        return float(speed_raw)
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("""
            SELECT speed_kmh FROM predictions
            WHERE bus_id = %s AND speed_kmh > 0
            ORDER BY created_at DESC LIMIT 1
        """, (bus_id,))
        row = cur.fetchone()
        conn.close()
        if row:
            return float(row[0])
    except:
        pass
    return 1.0

# ============================================================
# BUSES
# ============================================================

@app.get("/buses")
def get_buses():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT id, name, active, created_at FROM buses ORDER BY created_at")
    rows = cur.fetchall()
    conn.close()
    return [{"id":r[0],"name":r[1],"active":r[2],"created_at":str(r[3])} for r in rows]

@app.post("/buses")
def add_bus(bus: BusIn,
            x_user:     Optional[str] = Header(None),
            x_password: Optional[str] = Header(None)):
    check_admin(x_user, x_password)
    bus_id = bus.id.strip().upper()
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT id FROM buses WHERE id = %s", (bus_id,))
    if cur.fetchone():
        conn.close()
        raise HTTPException(status_code=400, detail="Bus deja enregistre")
    cur.execute(
        "INSERT INTO buses (id, name) VALUES (%s,%s) RETURNING id, name, active, created_at",
        (bus_id, bus.name)
    )
    row = cur.fetchone()
    conn.commit()
    conn.close()
    print(f"[API] Bus ajoute: {bus_id} - {bus.name}")
    return {"id":row[0],"name":row[1],"active":row[2],"created_at":str(row[3])}

@app.delete("/buses/{bus_id}")
def delete_bus(bus_id: str,
               x_user:     Optional[str] = Header(None),
               x_password: Optional[str] = Header(None)):
    check_admin(x_user, x_password)
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("DELETE FROM buses WHERE id = %s", (bus_id,))
    conn.commit()
    conn.close()
    return {"message": f"Bus {bus_id} supprime"}

# ============================================================
# LOGIN / SIGNUP
# ============================================================

class LoginIn(BaseModel):
    username: str
    password: str

@app.post("/login")
def login(data: LoginIn):
    user = get_user(data.username, data.password)
    if not user:
        raise HTTPException(status_code=401, detail="Identifiants incorrects")
    return {"username": user["username"], "role": user["role"]}

@app.post("/signup")
def signup(data: LoginIn):
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT id FROM users WHERE username = %s", (data.username,))
    if cur.fetchone():
        conn.close()
        raise HTTPException(status_code=400, detail="Nom d utilisateur deja pris")
    cur.execute(
        "INSERT INTO users (username, password, role) VALUES (%s,%s,'user') RETURNING id,username,role",
        (data.username, data.password)
    )
    row = cur.fetchone()
    conn.commit()
    conn.close()
    print(f"[API] Nouveau compte: {data.username}")
    return {"id":row[0],"username":row[1],"role":row[2]}

# ============================================================
# USERS
# ============================================================

class UserIn(BaseModel):
    username: str
    password: str
    role:     str = "user"

@app.get("/users")
def get_users(x_user: Optional[str] = Header(None),
              x_password: Optional[str] = Header(None)):
    check_admin(x_user, x_password)
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT id, username, role, created_at FROM users ORDER BY id")
    rows = cur.fetchall()
    conn.close()
    return [{"id":r[0],"username":r[1],"role":r[2],"created_at":str(r[3])} for r in rows]

@app.post("/users")
def add_user(user: UserIn,
             x_user:     Optional[str] = Header(None),
             x_password: Optional[str] = Header(None)):
    check_admin(x_user, x_password)
    if user.role not in ["admin", "user"]:
        raise HTTPException(status_code=400, detail="Role invalide")
    conn = get_db()
    cur  = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO users (username, password, role) VALUES (%s,%s,%s) RETURNING id,username,role,created_at",
            (user.username, user.password, user.role)
        )
        row = cur.fetchone()
        conn.commit()
        conn.close()
        print(f"[API] Utilisateur ajoute: {user.username} ({user.role})")
        return {"id":row[0],"username":row[1],"role":row[2],"created_at":str(row[3])}
    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=400, detail="Utilisateur deja existant")

@app.delete("/users/{username}")
def delete_user(username: str,
                x_user:     Optional[str] = Header(None),
                x_password: Optional[str] = Header(None)):
    check_admin(x_user, x_password)
    if username == "admin":
        raise HTTPException(status_code=400, detail="Impossible de supprimer admin")
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("DELETE FROM users WHERE username = %s", (username,))
    conn.commit()
    conn.close()
    return {"message": f"Utilisateur {username} supprime"}

# ============================================================
# STATIONS
# ============================================================

@app.get("/stations")
def get_stations():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("SELECT id, name, lat, lng, radius_km, created_at FROM stations ORDER BY id")
    rows = cur.fetchall()
    conn.close()
    return [{"id":r[0],"name":r[1],"lat":r[2],"lng":r[3],"radius_km":r[4],"created_at":str(r[5])} for r in rows]

@app.post("/stations")
def add_station(station: StationIn,
                x_user:     Optional[str] = Header(None),
                x_password: Optional[str] = Header(None)):
    check_admin(x_user, x_password)
    conn = get_db()
    cur  = conn.cursor()
    cur.execute(
        "INSERT INTO stations (name, lat, lng, radius_km) VALUES (%s,%s,%s,%s) RETURNING id, name, lat, lng, radius_km, created_at",
        (station.name, station.lat, station.lng, station.radius_km)
    )
    row = cur.fetchone()
    conn.commit()
    conn.close()
    print(f"[API] Station ajoutee: {station.name} ({station.lat},{station.lng})")
    return {"id":row[0],"name":row[1],"lat":row[2],"lng":row[3],"radius_km":row[4],"created_at":str(row[5])}

@app.delete("/stations/{station_id}")
def delete_station(station_id: int,
                   x_user:     Optional[str] = Header(None),
                   x_password: Optional[str] = Header(None)):
    check_admin(x_user, x_password)
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("DELETE FROM stations WHERE id = %s", (station_id,))
    conn.commit()
    conn.close()
    return {"message": f"Station {station_id} supprimee"}

# ============================================================
# ROOT + LATEST
# ============================================================

@app.get("/")
def root():
    return {"message": "Bus Prediction API", "status": "running"}

GPS_TIMEOUT_MINUTES = 5

@app.get("/latest")
def get_latest():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("""
        SELECT DISTINCT ON (p.bus_id)
            p.bus_id, p.segment, p.bus_stop,
            p.lat, p.lng, p.altitude, p.speed_kmh,
            p.eta_seconds, p.arrival_datetime, p.created_at,
            b.name as bus_name
        FROM predictions p
        INNER JOIN buses b ON b.id = p.bus_id AND b.active = true
        WHERE p.created_at >= NOW() - INTERVAL %s
        ORDER BY p.bus_id, p.created_at DESC
    """, (f"{GPS_TIMEOUT_MINUTES} minutes",))
    rows = cur.fetchall()
    conn.close()
    return [{"bus_id":r[0],"segment":r[1],"bus_stop":r[2],
             "lat":r[3],"lng":r[4],"altitude":r[5],
             "speed_kmh":r[6],"eta_seconds":r[7],
             "arrival_datetime":str(r[8]),"created_at":str(r[9]),
             "bus_name":r[10]} for r in rows]

# ============================================================
# ETA — MODELE HYBRIDE
# Physique  : vitesse > 30 km/h  OU  distance < 400 m
# XGBoost   : sinon (segments + arrets intermediaires)
# ============================================================

@app.get("/eta")
def get_eta(station_lat: float, station_lng: float, station_name: str = "unknown"):
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("""
        SELECT DISTINCT ON (p.bus_id)
            p.bus_id, p.segment, p.bus_stop,
            p.lat, p.lng, p.speed_kmh, p.hour, p.created_at
        FROM predictions p
        INNER JOIN buses b ON b.id = p.bus_id AND b.active = true
        WHERE p.lat IS NOT NULL AND p.lng IS NOT NULL
        AND p.created_at >= NOW() - INTERVAL '5 minutes'
        ORDER BY p.bus_id, p.created_at DESC
    """)
    rows = cur.fetchall()

    cur.execute("SELECT id, name, lat, lng, radius_km FROM stations ORDER BY id")
    all_stations = [{"id":s[0],"name":s[1],"lat":s[2],"lng":s[3],"radius_km":s[4]}
                    for s in cur.fetchall()]
    conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail="Aucun bus actif enregistre")

    results = []
    for r in rows:
        bus_id  = r[0]
        segment = r[1] or 1
        bus_lat = r[3]
        bus_lng = r[4]
        hour    = r[6] if r[6] else datetime.now().hour
        created = r[7]

        # Vitesse reelle GPS
        speed = get_real_speed(bus_id, r[5])

        # Distance totale bus -> station cible
        total_dist_km = haversine(bus_lat, bus_lng, station_lat, station_lng)
        total_dist_m  = round(total_dist_km * 1000, 0)

        # ─────────────────────────────────────────────────────
        # MODELE HYBRIDE
        # Condition physique : vitesse > 30 km/h OU dist < 400 m
        # → calcul direct distance/vitesse, rapide et precis
        # Sinon → pipeline XGBoost complet
        # ─────────────────────────────────────────────────────
        use_physics = (speed > 30) or (total_dist_m < 400)

        if use_physics:
            # Approche physique : eta = distance(m) / vitesse(m/s)
            speed_ms         = max(speed, 1.0) / 3.6
            eta_seconds      = max(1, int(round(total_dist_km * 1000 / speed_ms)))
            total_run_time   = float(eta_seconds)
            total_dwell_time = 0.0
            intermediate_stops_count = 0
            method = "physique"

            print(f"[ETA][PHYSIQUE] bus={bus_id} vitesse={speed}km/h "
                  f"dist={total_dist_m}m eta={eta_seconds}s ({eta_seconds//60}min)")

        else:
            # Pipeline XGBoost complet

            # Etape 1 : Diviser en segments de 1.5km
            sub_segments = split_distance_into_segments(total_dist_km, segment_size_km=1.5)

            # Etape 2 : run_time par segment avec XGBoost
            total_run_time = 0.0
            for seg_km in sub_segments:
                total_run_time += predict_run_time_for_segment(seg_km, speed, hour)

            # Etape 3 : Arrets intermediaires par interpolation
            n_interp    = max(10, int(total_dist_km * 2))
            path_points = interpolate_points(bus_lat, bus_lng, station_lat, station_lng, n_interp)

            visited_stops      = set()
            intermediate_stops = []
            for pt_lat, pt_lng in path_points:
                for s in all_stations:
                    if s["name"] == station_name: continue
                    if s["name"] in visited_stops: continue
                    if haversine(pt_lat, pt_lng, s["lat"], s["lng"]) <= s["radius_km"]:
                        visited_stops.add(s["name"])
                        intermediate_stops.append(s["name"])

            # Etape 4 : dwell_time par arret avec XGBoost
            total_dwell_time = 0.0
            for stop_name in intermediate_stops:
                total_dwell_time += predict_dwell_time_for_stop(stop_name, hour, segment)
            total_dwell_time += predict_dwell_time_for_stop(station_name, hour, segment)

            # Etape 5 : Somme
            eta_seconds = max(0, int(round(total_run_time + total_dwell_time)))
            intermediate_stops_count = len(intermediate_stops)
            method = "xgboost"

            print(f"[ETA][XGBOOST] bus={bus_id} vitesse={speed}km/h dist={total_dist_km:.1f}km "
                  f"segments={len(sub_segments)} arrets_interm={intermediate_stops_count} "
                  f"run={total_run_time:.0f}s dwell={total_dwell_time:.0f}s "
                  f"eta={eta_seconds}s ({eta_seconds//60}min)")

        arrival_dt = datetime.now() + timedelta(seconds=eta_seconds)

        results.append({
            "bus_id":             bus_id,
            "bus_lat":            bus_lat,
            "bus_lng":            bus_lng,
            "speed_kmh":          round(speed, 2),
            "distance_km":        round(total_dist_km, 3),
            "distance_m":         total_dist_m,
            "intermediate_stops": intermediate_stops_count,
            "run_time_sec":       round(total_run_time, 1),
            "dwell_time_sec":     round(total_dwell_time, 1),
            "eta_seconds":        eta_seconds,
            "eta_minutes":        round(eta_seconds / 60, 1),
            "arrival_time":       arrival_dt.strftime("%H:%M:%S"),
            "station_name":       station_name,
            "method":             method,
            "last_update":        str(created)
        })

    results.sort(key=lambda x: x["eta_seconds"])
    return results[0]

# ============================================================
# AUTRES ENDPOINTS
# ============================================================

@app.get("/predictions/{bus_id}")
def get_by_bus(bus_id: str, limit: int = 20):
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("""
        SELECT bus_id, segment, bus_stop, lat, lng, altitude,
               speed_kmh, eta_seconds, arrival_datetime, created_at
        FROM predictions WHERE bus_id = %s
        ORDER BY created_at DESC LIMIT %s
    """, (bus_id, limit))
    rows = cur.fetchall()
    conn.close()
    if not rows:
        raise HTTPException(status_code=404, detail="Bus non trouve")
    return [{"bus_id":r[0],"segment":r[1],"bus_stop":r[2],
             "lat":r[3],"lng":r[4],"altitude":r[5],
             "speed_kmh":r[6],"eta_seconds":r[7],
             "arrival_datetime":str(r[8]),"created_at":str(r[9])} for r in rows]

@app.get("/history")
def get_history(limit: int = 100):
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("""
        SELECT bus_id, segment, bus_stop, lat, lng, altitude,
               speed_kmh, eta_seconds, arrival_datetime, created_at
        FROM predictions ORDER BY created_at DESC LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    conn.close()
    return [{"bus_id":r[0],"segment":r[1],"bus_stop":r[2],
             "lat":r[3],"lng":r[4],"altitude":r[5],
             "speed_kmh":r[6],"eta_seconds":r[7],
             "arrival_datetime":str(r[8]),"created_at":str(r[9])} for r in rows]

@app.get("/stats")
def get_stats():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("""
        SELECT bus_id, COUNT(*),
               ROUND(AVG(speed_kmh)::numeric,2),
               ROUND(AVG(eta_seconds)::numeric,0),
               MIN(created_at), MAX(created_at)
        FROM predictions GROUP BY bus_id
    """)
    rows = cur.fetchall()
    conn.close()
    return [{"bus_id":r[0],"total_messages":r[1],
             "vitesse_moyenne":float(r[2]) if r[2] else 0,
             "eta_moyen":float(r[3]) if r[3] else 0,
             "premier_message":str(r[4]),"dernier_message":str(r[5])} for r in rows]

@app.get("/latency")
def get_latency():
    """Mesure la latence entre la derniere donnee GPS et maintenant"""
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("""
        SELECT bus_id, created_at, NOW() as now,
               EXTRACT(EPOCH FROM (NOW() - created_at)) as latency_sec
        FROM predictions
        ORDER BY created_at DESC LIMIT 5
    """)
    rows = cur.fetchall()
    conn.close()
    return [
        {
            "bus_id":      r[0],
            "last_data":   str(r[1]),
            "now":         str(r[2]),
            "latency_sec": round(float(r[3]), 2),
            "latency_ms":  round(float(r[3]) * 1000, 0)
        }
        for r in rows
    ]

@app.get("/history/map")
def get_map_data():
    conn = get_db()
    cur  = conn.cursor()
    cur.execute("""
        SELECT bus_id, lat, lng, altitude, speed_kmh, created_at
        FROM predictions WHERE lat IS NOT NULL AND lng IS NOT NULL
        ORDER BY created_at DESC LIMIT 500
    """)
    rows = cur.fetchall()
    conn.close()
    return [{"bus_id":r[0],"lat":r[1],"lng":r[2],
             "altitude":r[3],"speed_kmh":r[4],"time":str(r[5])} for r in rows]