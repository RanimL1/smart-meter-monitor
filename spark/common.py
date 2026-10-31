"""Configuration et fonctions partagées par les jobs Spark."""
import os
from pyspark.sql import SparkSession

# --- Chemins (HDFS par défaut ; remplacez BASE_PATH par file:///tmp/x pour un test local)
BASE = os.getenv("BASE_PATH", "hdfs://namenode:8020")
RAW_PATH = f"{BASE}/data/raw/household_power_consumption.txt"
CLEAN_PATH = f"{BASE}/data/clean/consumption"        # Parquet partitionné par année
REF_PATH = f"{BASE}/data/reference/hourly_profile"   # Parquet : profil de référence
CHECKPOINT = f"{BASE}/checkpoints/streaming_job"

# --- Kafka / MongoDB
KAFKA_SERVERS = os.getenv("KAFKA_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "power_readings")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "smartmeter")

# --- Paramètres métier
# Le profil de référence est calculé sur les données STRICTEMENT antérieures à cette date ;
# le rejeu en streaming commence à cette date (pas de fuite entre référence et test).
REF_END = os.getenv("REF_END", "2010-01-01")
Z_THRESHOLD = float(os.getenv("Z_THRESHOLD", "3.0"))
MIN_POINTS = int(os.getenv("MIN_POINTS", "30"))      # mesures minimales par fenêtre d'une heure


def get_spark(name: str) -> SparkSession:
    spark = (SparkSession.builder.appName(name)
             .config("spark.sql.session.timeZone", "UTC")   # évite tout décalage horaire
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark
