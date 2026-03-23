#include <TinyGPS++.h>
#include <WiFiS3.h>
#include <ArduinoJson.h>

const char* ssid       = "Redmi 12";
const char* password   = "salwa1971";
const char* serverIP   = "10.236.117.119";
const int   serverPort = 8082;

WiFiClient  client;
TinyGPSPlus gps;

String busId             = "BUS_001";
int    segmentId         = 1;
unsigned long lastSend   = 0;
const unsigned long SEND_INTERVAL = 5000;

void setup() {
  Serial.begin(9600);
  Serial1.begin(9600);

  Serial.println("=== Bus GPS Tracker ===");
  Serial.println("Init module SIM808...");
  delay(2000);

  Serial1.println("AT+CGNSPWR=1");
  delay(1000);
  Serial1.println("AT+CGNSTST=1");
  delay(500);

  Serial.print("Connexion WiFi");
  WiFi.begin(ssid, password);
  int tentatives = 0;
  while (WiFi.status() != WL_CONNECTED && tentatives < 30) {
    delay(500);
    Serial.print(".");
    tentatives++;
  }

  if (WiFi.status() == WL_CONNECTED) {
    Serial.println("\nWiFi OK !");
    Serial.print("IP Arduino : ");
    Serial.println(WiFi.localIP());
  } else {
    Serial.println("\nECHEC WiFi — verifie SSID/password");
  }
}

void loop() {
  while (Serial1.available()) {
    char c = Serial1.read();
    gps.encode(c);
  }

  unsigned long now = millis();

  if (gps.location.isValid() && (now - lastSend >= SEND_INTERVAL)) {
    lastSend = now;

    float speed = gps.speed.kmph();
    if (speed < 0.5) speed = 0;

    int h = gps.time.hour();
    int m = gps.time.minute();
    int s = gps.time.second();

    char timestamp[25];
    snprintf(timestamp, sizeof(timestamp),
             "%04d-%02d-%02dT%02d:%02d:%02dZ",
             gps.date.year(), gps.date.month(), gps.date.day(),
             h, m, s);

    StaticJsonDocument<300> doc;
    doc["bus_id"]     = busId;
    doc["segment"]    = segmentId;
    doc["lat"]        = gps.location.lat();
    doc["lng"]        = gps.location.lng();
    doc["altitude"]   = gps.altitude.meters();
    doc["speed_kmh"]  = speed;
    doc["hour"]       = h;
    doc["timestamp"]  = timestamp;
    doc["satellites"] = gps.satellites.value();

    String payload;
    serializeJson(doc, payload);
    sendToKafka(payload);
  }

  if (!gps.location.isValid() && (now - lastSend >= SEND_INTERVAL)) {
    lastSend = now;
    Serial.println("Attente signal GPS...");
    Serial.print("Satellites: ");
    Serial.println(gps.satellites.value());
  }
}

void sendToKafka(String jsonPayload) {
  if (WiFi.status() != WL_CONNECTED) {
    Serial.println("WiFi perdu — reconnexion...");
    WiFi.begin(ssid, password);
    delay(3000);
    return;
  }

  if (!client.connect(serverIP, serverPort)) {
    Serial.println("ERREUR: impossible de contacter le serveur");
    return;
  }

  String body = "{\"records\":[{\"value\":" + jsonPayload + "}]}";

  client.println("POST /topics/gps-raw HTTP/1.1");
  client.println("Host: " + String(serverIP));
  client.println("Content-Type: application/vnd.kafka.json.v2+json");
  client.println("Content-Length: " + String(body.length()));
  client.println("Connection: close");
  client.println();
  client.println(body);

  delay(500);

  while (client.available()) {
    String line = client.readStringUntil('\n');
    if (line.startsWith("HTTP/1.1")) {
      Serial.println("Serveur: " + line);
    }
  }

  client.stop();
  Serial.println("Envoye: " + jsonPayload);
}
