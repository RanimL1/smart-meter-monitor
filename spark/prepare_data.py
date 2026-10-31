"""Étape 1 (batch) : nettoie le fichier brut sur HDFS et l'écrit en Parquet.

- séparateur ';', '?' = valeur manquante
- construit un timestamp à partir de Date (j/m/aaaa) et Time
- supprime les lignes sans puissance active
- partitionne par année (lecture sélective rapide)
"""
from pyspark.sql import functions as F
from common import get_spark, RAW_PATH, CLEAN_PATH

NUM_COLS = {
    "Global_active_power": "active_power",       # kW
    "Global_reactive_power": "reactive_power",   # kVAR
    "Voltage": "voltage",                        # V
    "Global_intensity": "intensity",             # A
    "Sub_metering_1": "sub1",                    # Wh (cuisine)
    "Sub_metering_2": "sub2",                    # Wh (buanderie)
    "Sub_metering_3": "sub3",                    # Wh (chauffe-eau, clim)
}


def clean(raw):
    df = raw.withColumn(
        "ts", F.to_timestamp(F.concat_ws(" ", "Date", "Time"), "d/M/yyyy HH:mm:ss"))
    for src, dst in NUM_COLS.items():
        df = df.withColumn(dst, F.col(src).cast("double"))   # '?' -> null (nullValue)
    df = df.select("ts", *NUM_COLS.values())
    return (df.filter(F.col("ts").isNotNull() & F.col("active_power").isNotNull())
              .withColumn("year", F.year("ts")))


if __name__ == "__main__":
    spark = get_spark("prepare_data")
    raw = (spark.read.option("header", True).option("sep", ";")
           .option("nullValue", "?").csv(RAW_PATH))
    n_raw = raw.count()
    out = clean(raw)
    out.write.mode("overwrite").partitionBy("year").parquet(CLEAN_PATH)
    n_ok = spark.read.parquet(CLEAN_PATH).count()
    print(f"[prepare_data] lignes brutes={n_raw}  lignes valides={n_ok}  "
          f"rejetées={n_raw - n_ok}")
    spark.stop()
