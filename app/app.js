const API = "http://localhost:8000";

// ===================== LANDING NAVIGATION =====================
function goToLogin() {
  document.getElementById("landing-screen").style.display = "none";
  document.getElementById("auth-screen").style.display   = "flex";
  showLogin();
}

function goToSignup() {
  document.getElementById("landing-screen").style.display = "none";
  document.getElementById("auth-screen").style.display   = "flex";
  showSignup();
}

function backToLanding() {
  document.getElementById("auth-screen").style.display    = "none";
  document.getElementById("landing-screen").style.display = "block";
  document.getElementById("landing-screen").scrollTop     = 0;
}

let currentUser    = null;
let map            = null;
let busMarkers     = {};
let stationMarkers = {};
let stations       = [];
let buses          = [];
let latestData     = [];
let etaInterval    = null;
let currentStation = null;

// ===================== AUTH =====================
function showLogin() {
  document.getElementById("login-form").style.display  = "block";
  document.getElementById("signup-form").style.display = "none";
  document.getElementById("btn-login-tab").classList.add("active");
  document.getElementById("btn-signup-tab").classList.remove("active");
  document.getElementById("login-error").textContent   = "";
}

function showSignup() {
  document.getElementById("login-form").style.display  = "none";
  document.getElementById("signup-form").style.display = "block";
  document.getElementById("btn-signup-tab").classList.add("active");
  document.getElementById("btn-login-tab").classList.remove("active");
  document.getElementById("signup-error").textContent   = "";
  document.getElementById("signup-success").textContent = "";
}

async function login() {
  const u = document.getElementById("login-username").value.trim();
  const p = document.getElementById("login-password").value.trim();
  if (!u || !p) {
    document.getElementById("login-error").textContent = "Remplissez tous les champs";
    return;
  }
  try {
    const res = await fetch(`${API}/login`, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ username: u, password: p })
    });
    if (!res.ok) {
      document.getElementById("login-error").textContent = "Identifiants incorrects";
      return;
    }
    const data = await res.json();
    currentUser = { name: data.username, role: data.role, password: p };
    document.getElementById("auth-screen").style.display = "none";
    document.getElementById("app-screen").style.display  = "flex";
    document.getElementById("user-role-badge").textContent = data.role.toUpperCase();
    initApp();
  } catch(e) {
    document.getElementById("login-error").textContent = "Erreur connexion API";
  }
}

async function signup() {
  const u  = document.getElementById("signup-username").value.trim();
  const p  = document.getElementById("signup-password").value.trim();
  const p2 = document.getElementById("signup-confirm").value.trim();
  document.getElementById("signup-error").textContent   = "";
  document.getElementById("signup-success").textContent = "";

  if (!u || !p || !p2) {
    document.getElementById("signup-error").textContent = "Remplissez tous les champs";
    return;
  }
  if (p !== p2) {
    document.getElementById("signup-error").textContent = "Les mots de passe ne correspondent pas";
    return;
  }
  if (p.length < 4) {
    document.getElementById("signup-error").textContent = "Mot de passe trop court (4 caracteres minimum)";
    return;
  }
  try {
    const res = await fetch(`${API}/signup`, {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ username: u, password: p, role: "user" })
    });
    if (!res.ok) {
      const e = await res.json();
      document.getElementById("signup-error").textContent = e.detail || "Erreur creation compte";
      return;
    }
    document.getElementById("signup-success").textContent = "Compte cree ! Vous pouvez vous connecter.";
    document.getElementById("signup-username").value = "";
    document.getElementById("signup-password").value = "";
    document.getElementById("signup-confirm").value  = "";
    setTimeout(() => showLogin(), 2000);
  } catch(e) {
    document.getElementById("signup-error").textContent = "Erreur connexion API";
  }
}

function logout() {
  currentUser = null;
  if (etaInterval) clearInterval(etaInterval);
  document.getElementById("app-screen").style.display    = "none";
  document.getElementById("auth-screen").style.display   = "none";
  document.getElementById("landing-screen").style.display = "block";
  document.getElementById("landing-screen").scrollTop     = 0;
}

// ===================== INIT =====================
async function initApp() {
  if (currentUser.role === "admin") {
    document.getElementById("admin-panel").style.display = "block";
  } else {
    document.getElementById("admin-panel").style.display = "none";
  }
  initMap();
  await loadStationsFromAPI();
  await loadBusesFromAPI();
  if (currentUser.role === "admin") await loadUsersFromAPI();
  fetchData();
  setInterval(fetchData, 5000);
  setInterval(loadStationsFromAPI, 30000);
  setInterval(loadBusesFromAPI, 30000);
}

// ===================== MAP =====================
function initMap() {
  map = L.map("map").setView([33.8820, 10.0980], 8);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "OpenStreetMap"
  }).addTo(map);
  map.on("click", function(e) {
    if (currentUser.role !== "admin") return;
    const name = document.getElementById("new-station-name").value.trim();
    if (!name) { alert("Entrez d abord le nom de la station"); return; }
    document.getElementById("new-station-lat").value = e.latlng.lat.toFixed(6);
    document.getElementById("new-station-lng").value = e.latlng.lng.toFixed(6);
    addStation();
  });
}

// ===================== ICONES =====================
function createBusIcon(busId, speed) {
  const moving = speed > 0;
  const color  = moving ? "#00d4aa" : "#888888";
  const cls    = moving ? "bus-icon-wrap" : "bus-stopped";
  return L.divIcon({
    className: "",
    html: `<div class="${cls}">
      <svg width="52" height="38" viewBox="0 0 52 38" xmlns="http://www.w3.org/2000/svg">
        <rect x="1" y="2" width="50" height="26" rx="7" fill="${color}" stroke="#fff" stroke-width="1.5"/>
        <rect x="5" y="6" width="11" height="9" rx="2" fill="rgba(255,255,255,0.85)"/>
        <rect x="20" y="6" width="11" height="9" rx="2" fill="rgba(255,255,255,0.85)"/>
        <rect x="35" y="6" width="11" height="9" rx="2" fill="rgba(255,255,255,0.85)"/>
        <rect x="3" y="20" width="46" height="5" rx="2" fill="rgba(0,0,0,0.2)"/>
        <circle cx="11" cy="34" r="4" fill="#222" stroke="#fff" stroke-width="1.5"/>
        <circle cx="41" cy="34" r="4" fill="#222" stroke="#fff" stroke-width="1.5"/>
        <text x="26" y="18" text-anchor="middle" font-size="5.5" fill="white" font-weight="bold">${busId}</text>
      </svg>
    </div>`,
    iconSize:[52,38], iconAnchor:[26,34]
  });
}

function createStationIcon() {
  return L.divIcon({
    className: "",
    html: `<div class="station-icon-wrap">
      <svg width="34" height="42" viewBox="0 0 34 42" xmlns="http://www.w3.org/2000/svg">
        <path d="M17 0 C7.6 0 0 7.6 0 17 C0 29.5 17 42 17 42 C17 42 34 29.5 34 17 C34 7.6 26.4 0 17 0Z"
              fill="#f39c12" stroke="#fff" stroke-width="1.5"/>
        <circle cx="17" cy="17" r="9" fill="white"/>
        <rect x="13" y="10" width="8" height="13" rx="2" fill="#f39c12"/>
        <rect x="11" y="19" width="12" height="3.5" rx="1.5" fill="#f39c12"/>
        <circle cx="14" cy="25" r="2" fill="#e67e22"/>
        <circle cx="20" cy="25" r="2" fill="#e67e22"/>
      </svg>
    </div>`,
    iconSize:[34,42], iconAnchor:[17,42]
  });
}

// ===================== USERS =====================
async function loadUsersFromAPI() {
  try {
    const res = await fetch(`${API}/users`, {
      headers: { "x-user": currentUser.name, "x-password": currentUser.password }
    });
    const users = await res.json();
    renderUserList(users);
  } catch(e) { console.warn("Users non disponibles"); }
}

function renderUserList(users) {
  const list = document.getElementById("user-list");
  if (!list) return;
  list.innerHTML = users.map(u =>
    `<div class="user-card">
      <div>
        <span class="uc-name">${u.username}</span>
        <span class="uc-role ${u.role}">${u.role}</span>
      </div>
      ${u.username !== "admin"
        ? `<button class="uc-del" onclick='deleteUser("${u.username}")'>&#10005;</button>`
        : ""}
    </div>`
  ).join("") || "<p style='color:#555;font-size:12px'>Aucun utilisateur</p>";
}

async function deleteUser(username) {
  if (!confirm(`Supprimer l utilisateur ${username} ?`)) return;
  await fetch(`${API}/users/${username}`, {
    method:"DELETE",
    headers:{ "x-user":currentUser.name, "x-password":currentUser.password }
  });
  await loadUsersFromAPI();
}

// ===================== STATIONS =====================
async function loadStationsFromAPI() {
  try {
    const res = await fetch(`${API}/stations`);
    stations  = await res.json();
    renderStations();
  } catch(e) { console.warn("Stations non disponibles"); }
}

function renderStations() {
  Object.values(stationMarkers).forEach(m => map.removeLayer(m));
  stationMarkers = {};

  const sidebarAdmin = document.getElementById("station-list-admin");
  const sidebarUser  = document.getElementById("stations-sidebar");

  if (sidebarAdmin) {
    sidebarAdmin.innerHTML = stations.map(s =>
      `<div class="station-card">
        <span class="sc-name">${s.name}</span>
        <button class="sc-del" onclick='deleteStation(${s.id})'>&#10005;</button>
      </div>`
    ).join("") || "<p style='color:#555;font-size:12px'>Aucune station</p>";
  }

  if (sidebarUser) {
    sidebarUser.innerHTML = stations.map(s =>
      `<div class="station-card" onclick='showETA(${JSON.stringify(s).replace(/'/g,"&#39;")})'>
        <span class="sc-name">${s.name}</span>
      </div>`
    ).join("") || "<p style='color:#555;font-size:12px'>Aucune station</p>";
  }

  stations.forEach(s => {
    const icon   = createStationIcon();
    const marker = L.marker([s.lat, s.lng], { icon })
      .addTo(map)
      .bindTooltip(s.name, { direction:"top", offset:[0,-42] })
      .on("click", () => showETA(s));
    stationMarkers[s.id] = marker;
  });
}

async function addStation() {
  const name      = document.getElementById("new-station-name").value.trim();
  const lat       = parseFloat(document.getElementById("new-station-lat").value);
  const lng       = parseFloat(document.getElementById("new-station-lng").value);
  const radius_km = parseFloat(document.getElementById("new-station-radius").value) || 0.3;
  if (!name || isNaN(lat) || isNaN(lng)) { alert("Remplissez nom, latitude et longitude"); return; }
  try {
    const res = await fetch(`${API}/stations`, {
      method:"POST",
      headers:{ "Content-Type":"application/json", "x-user":currentUser.name, "x-password":currentUser.password },
      body:JSON.stringify({ name, lat, lng, radius_km })
    });
    if (!res.ok) { const e=await res.json(); alert(e.detail); return; }
    document.getElementById("new-station-name").value   = "";
    document.getElementById("new-station-lat").value    = "";
    document.getElementById("new-station-lng").value    = "";
    document.getElementById("new-station-radius").value = "";
    await loadStationsFromAPI();
  } catch(e) { alert("Erreur: " + e.message); }
}

async function deleteStation(id) {
  if (!confirm("Supprimer cette station ?")) return;
  await fetch(`${API}/stations/${id}`, {
    method:"DELETE",
    headers:{ "x-user":currentUser.name, "x-password":currentUser.password }
  });
  await loadStationsFromAPI();
}

// ===================== BUS =====================
async function loadBusesFromAPI() {
  try {
    const res = await fetch(`${API}/buses`);
    buses     = await res.json();
    renderBusList();
  } catch(e) { console.warn("Bus non disponibles"); }
}

async function addBus() {
  const id   = document.getElementById("new-bus-id").value.trim().toUpperCase();
  const name = document.getElementById("new-bus-name").value.trim();
  if (!id || !name) { alert("Remplissez ID et nom"); return; }
  try {
    const res = await fetch(`${API}/buses`, {
      method:"POST",
      headers:{ "Content-Type":"application/json", "x-user":currentUser.name, "x-password":currentUser.password },
      body:JSON.stringify({ id, name })
    });
    if (!res.ok) { const e=await res.json(); alert(e.detail); return; }
    document.getElementById("new-bus-id").value   = "";
    document.getElementById("new-bus-name").value = "";
    await loadBusesFromAPI();
  } catch(e) { alert("Erreur: " + e.message); }
}

async function deleteBus(id) {
  if (!confirm(`Supprimer le bus ${id} ?`)) return;
  await fetch(`${API}/buses/${id}`, {
    method:"DELETE",
    headers:{ "x-user":currentUser.name, "x-password":currentUser.password }
  });
  if (busMarkers[id]) { map.removeLayer(busMarkers[id]); delete busMarkers[id]; }
  await loadBusesFromAPI();
}

function renderBusList() {
  const list = document.getElementById("bus-list");
  if (list) {
    list.innerHTML = buses.map(b =>
      `<div class="station-card">
        <span class="sc-name">${b.name} (${b.id})</span>
        <button class="sc-del" onclick='deleteBus("${b.id}")'>&#10005;</button>
       </div>`
    ).join("") || "<p style='color:#555;font-size:12px'>Aucun bus</p>";
  }
}

// ===================== FETCH DATA =====================
async function fetchData() {
  try {
    const res  = await fetch(`${API}/latest`);
    latestData = await res.json();
    updateBusMarkers(latestData);
    updateBusSidebar(latestData);
  } catch(e) { console.warn("API non disponible"); }
}

function updateBusMarkers(data) {
  const regIds = buses.map(b => b.id);
  data.forEach(bus => {
    if (!bus.lat || !bus.lng) return;
    if (regIds.length > 0 && !regIds.includes(bus.bus_id)) return;
    const icon = createBusIcon(bus.bus_id, bus.speed_kmh || 0);
    const tip  = `<b>${bus.bus_id}</b><br>Vitesse: ${bus.speed_kmh?bus.speed_kmh.toFixed(1):"0"} km/h<br>${bus.bus_stop||"En route"}`;
    if (busMarkers[bus.bus_id]) {
      busMarkers[bus.bus_id].setLatLng([bus.lat, bus.lng]);
      busMarkers[bus.bus_id].setIcon(icon);
    } else {
      busMarkers[bus.bus_id] = L.marker([bus.lat, bus.lng], { icon, zIndexOffset:1000 })
        .addTo(map).bindTooltip(tip, { direction:"top", offset:[0,-34] });
    }
  });
}

function updateBusSidebar(data) {
  const sb = document.getElementById("buses-sidebar");
  if (!sb) return;
  const regIds   = buses.map(b => b.id);
  const filtered = regIds.length > 0 ? data.filter(b => regIds.includes(b.bus_id)) : data;
  if (filtered.length === 0) {
    sb.innerHTML = "<p style='color:#555;padding:8px 16px;font-size:12px'>Aucun bus actif</p>";
    return;
  }
  sb.innerHTML = filtered.map(bus => {
    const info   = buses.find(b => b.id === bus.bus_id);
    const name   = info ? info.name : bus.bus_id;
    const speed  = bus.speed_kmh ? bus.speed_kmh.toFixed(1) : "0";
    const moving = parseFloat(speed) > 0;
    const color  = moving ? "#00d4aa" : "#888";
    return `<div class="bus-card" onclick="focusBus('${bus.bus_id}')">
      <div class="bc-id">${bus.bus_id} — ${name}</div>
      <div class="bc-status" style="color:${color}">${moving?"En circulation":"A l arret"}</div>
      <div class="bc-speed">Vitesse: ${speed} km/h | Stop: ${bus.bus_stop||"--"}</div>
      <div class="bc-pos">Lat: ${bus.lat?bus.lat.toFixed(5):"--"} | Lng: ${bus.lng?bus.lng.toFixed(5):"--"}</div>
    </div>`;
  }).join("");
}

function focusBus(busId) {
  const bus = latestData.find(b => b.bus_id === busId);
  if (bus && bus.lat && bus.lng) map.setView([bus.lat, bus.lng], 15);
}

// ===================== ETA =====================
async function showETA(station) {
  currentStation = station;
  document.getElementById("eta-station-name").textContent = station.name;
  document.getElementById("eta-popup").style.display      = "block";
  ["eta-bus-id","eta-dist","eta-time","eta-arrival","eta-speed","eta-run","eta-dwell"]
    .forEach(id => document.getElementById(id).textContent = "...");
  map.setView([station.lat, station.lng], 14);
  await refreshETA();
  if (etaInterval) clearInterval(etaInterval);
  etaInterval = setInterval(refreshETA, 10000);
}

async function refreshETA() {
  if (!currentStation) return;
  try {
    const url = `${API}/eta?station_lat=${currentStation.lat}&station_lng=${currentStation.lng}&station_name=${encodeURIComponent(currentStation.name)}`;
    const res = await fetch(url);
    if (!res.ok) {
      document.getElementById("eta-bus-id").textContent = "Aucun bus actif";
      document.getElementById("eta-time").textContent   = "--";
      return;
    }
    const d    = await res.json();
    const info = buses.find(b => b.id === d.bus_id);
    document.getElementById("eta-bus-id").textContent  = info ? `${d.bus_id} (${info.name})` : d.bus_id;
    document.getElementById("eta-dist").textContent    = d.distance_km + " km";

    // Convertir en minutes et secondes
    const totalSec = d.eta_seconds;
    const mins = Math.floor(totalSec / 60);
    const secs = totalSec % 60;
    const etaStr = mins > 0 ? mins + " min " + secs + " sec" : secs + " sec";
    document.getElementById("eta-time").textContent    = etaStr;
    document.getElementById("eta-arrival").textContent = d.arrival_time;
    document.getElementById("eta-speed").textContent   = parseFloat(d.speed_kmh) > 0
      ? d.speed_kmh + " km/h"
      : "A l arret";
    document.getElementById("eta-run").textContent   = Math.round(d.run_time_sec) + " s";
    document.getElementById("eta-dwell").textContent = Math.round(d.dwell_time_sec) + " s";

    // Label methode hybride
    const methodLabel = d.method === "physique"
      ? "Calcul physique (dist < 400 m ou vitesse > 20 km/h)"
      : "Modele IA XGBoost";
    document.getElementById("eta-update").textContent =
      methodLabel + " — " + new Date().toLocaleTimeString("fr-FR");

  } catch(e) {
    document.getElementById("eta-bus-id").textContent = "Erreur API";
  }
}

function closeEta() {
  document.getElementById("eta-popup").style.display = "none";
  if (etaInterval) clearInterval(etaInterval);
  currentStation = null;
}

// Enter pour login
document.addEventListener("DOMContentLoaded", () => {
  document.getElementById("login-password").addEventListener("keypress", e => {
    if (e.key === "Enter") login();
  });
  document.getElementById("signup-confirm").addEventListener("keypress", e => {
    if (e.key === "Enter") signup();
  });
});