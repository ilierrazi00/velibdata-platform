import json
import os
import time
import logging
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("weather-producer")

API_URL = "https://api.openweathermap.org/data/2.5/weather"
API_KEY = os.getenv("OWM_API_KEY", "")
CITY = os.getenv("OWM_CITY", "Paris,fr")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "velib.weather")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "600"))  # 10 min (respect du quota)


def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
            )
            log.info("Connecté à Kafka sur %s", KAFKA_BOOTSTRAP)
            return producer
        except NoBrokersAvailable:
            log.warning("Kafka indisponible, nouvelle tentative dans 5s...")
            time.sleep(5)


def fetch_weather():
    params = {"q": CITY, "appid": API_KEY, "units": "metric", "lang": "fr"}
    resp = requests.get(API_URL, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def main():
    if not API_KEY:
        log.error("OWM_API_KEY manquante. Renseignez-la dans le fichier .env")
    producer = create_producer()
    log.info("Démarrage du polling météo toutes les %ss", POLL_INTERVAL)
    while True:
        try:
            weather = fetch_weather()
            producer.send(KAFKA_TOPIC, value=weather)
            producer.flush()
            temp = weather.get("main", {}).get("temp")
            log.info("Météo Paris publiée : %s°C", temp)
        except requests.RequestException as e:
            log.error("Erreur API météo : %s", e)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()