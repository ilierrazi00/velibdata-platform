import json
import os
import time
import logging
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("velib-stations-producer")

API_BASE = (
    "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/"
    "velib-emplacement-des-stations/records"
)
PAGE_SIZE = 100
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "velib.station_information")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "600"))


def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8"),
                acks="all",
            )
            log.info("Connecté à Kafka sur %s", KAFKA_BOOTSTRAP)
            return producer
        except NoBrokersAvailable:
            log.warning("Kafka indisponible, nouvelle tentative dans 5s...")
            time.sleep(5)


def fetch_all_stations():
    stations, offset = [], 0
    while True:
        resp = requests.get(API_BASE, params={"limit": PAGE_SIZE, "offset": offset}, timeout=15)
        resp.raise_for_status()
        results = resp.json().get("results", [])
        if not results:
            break
        stations.extend(results)
        offset += PAGE_SIZE
        if offset >= 1500:
            break
    return stations


def main():
    producer = create_producer()
    log.info("Démarrage du polling stations toutes les %ss", POLL_INTERVAL)
    while True:
        try:
            stations = fetch_all_stations()
            for station in stations:
                key = station.get("stationcode", "unknown")
                producer.send(KAFKA_TOPIC, key=key, value=station)
            producer.flush()
            log.info("%d stations (référentiel) publiées dans '%s'", len(stations), KAFKA_TOPIC)
        except requests.RequestException as e:
            log.error("Erreur API stations : %s", e)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()