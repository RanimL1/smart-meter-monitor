"""Rejoue le fichier de consommation dans Kafka, minute par minute (flux simulé).

Chaque message est un JSON : {"ts": "2010-01-01T00:00:00", "active_power": 1.2, ...}
Le temps d'événement reste celui des données d'origine (Spark s'en sert pour les fenêtres).

Exemples :
  python replay_producer.py --rate 200                       # 200 mesures/seconde
  python replay_producer.py --rate 500 --anomaly-prob 0.02    # ~2 % des heures sont gonflées x3
  python replay_producer.py --dry-run --limit 5               # affiche sans Kafka
"""
import argparse
import csv
import json
import os
import random
import time
from datetime import datetime

COLS = ["Global_active_power", "Global_reactive_power", "Voltage",
        "Global_intensity", "Sub_metering_1", "Sub_metering_2", "Sub_metering_3"]
NAMES = ["active_power", "reactive_power", "voltage", "intensity", "sub1", "sub2", "sub3"]


def read_rows(path, start, end):
    """Génère les mesures valides entre start (inclus) et end (exclu)."""
    with open(path, newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            if row["Global_active_power"] == "?":
                continue                                    # valeur manquante : on saute
            ts = datetime.strptime(f'{row["Date"]} {row["Time"]}', "%d/%m/%Y %H:%M:%S")
            if ts < start:
                continue
            if end and ts >= end:
                return
            try:
                vals = {n: float(row[c]) for c, n in zip(COLS, NAMES)}
            except ValueError:
                continue
            yield {"ts": ts.strftime("%Y-%m-%dT%H:%M:%S"), **vals}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--file", default=os.getenv("DATA_FILE", "/data/household_power_consumption.txt"))
    p.add_argument("--servers", default=os.getenv("KAFKA_SERVERS", "kafka:9092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "power_readings"))
    p.add_argument("--start", default=os.getenv("REF_END", "2010-01-01"),
                   help="début du rejeu (par défaut = fin de la période de référence)")
    p.add_argument("--end", default=None, help="fin du rejeu (exclu), optionnel")
    p.add_argument("--rate", type=float, default=100, help="mesures par seconde")
    p.add_argument("--limit", type=int, default=0, help="nombre max de messages (0 = illimité)")
    p.add_argument("--anomaly-prob", type=float, default=0.0,
                   help="probabilité qu'une heure soit multipliée par --anomaly-factor")
    p.add_argument("--anomaly-factor", type=float, default=3.0)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--dry-run", action="store_true", help="affiche sans envoyer à Kafka")
    a = p.parse_args()

    start = datetime.fromisoformat(a.start)
    end = datetime.fromisoformat(a.end) if a.end else None
    rng = random.Random(a.seed)

    producer = None
    if not a.dry_run:
        from kafka import KafkaProducer
        producer = KafkaProducer(bootstrap_servers=a.servers,
                                 value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                                 linger_ms=20)

    sent, current_hour, hour_is_anomaly, n_anom = 0, None, False, 0
    delay = 1.0 / a.rate if a.rate > 0 else 0
    t_next = time.perf_counter()
    for msg in read_rows(a.file, start, end):
        hour_key = msg["ts"][:13]                           # 'aaaa-mm-jjThh'
        if hour_key != current_hour:                        # nouvelle heure : on tire au sort
            current_hour = hour_key
            hour_is_anomaly = rng.random() < a.anomaly_prob
            if hour_is_anomaly:
                n_anom += 1
                print(f"[producer] heure anormale injectée : {hour_key}h (x{a.anomaly_factor})")
        if hour_is_anomaly:
            msg["active_power"] = round(msg["active_power"] * a.anomaly_factor, 3)

        if a.dry_run:
            print(json.dumps(msg))
        else:
            producer.send(a.topic, msg)

        sent += 1
        if a.limit and sent >= a.limit:
            break
        if sent % 1000 == 0:
            print(f"[producer] {sent} messages envoyés (dernier ts : {msg['ts']})")
        t_next += delay                                     # cadence régulière sans dérive
        sleep = t_next - time.perf_counter()
        if sleep > 0:
            time.sleep(sleep)

    if producer:
        producer.flush()
    print(f"[producer] terminé : {sent} messages, {n_anom} heures anormales injectées")


if __name__ == "__main__":
    main()
