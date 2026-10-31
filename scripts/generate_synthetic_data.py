"""Génère un faux fichier au format exact du jeu UCI
« Individual household electric power consumption » (plan B si le
téléchargement échoue).

Format : séparateur ';', date j/m/aaaa, heure HH:MM:SS, '?' = valeur manquante.
Usage : python scripts/generate_synthetic_data.py --start 2007-01-01 --end 2010-06-30
"""
import argparse
import numpy as np
import pandas as pd


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2007-01-01")
    p.add_argument("--end", default="2010-06-30")
    p.add_argument("--out", default="data/household_power_consumption.txt")
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args()

    rng = np.random.default_rng(a.seed)
    idx = pd.date_range(a.start, a.end + " 23:59:00", freq="min")
    n = len(idx)
    hour = idx.hour.values + idx.minute.values / 60
    dow = idx.dayofweek.values  # 0 = lundi

    # Profil journalier : pic le matin et surtout le soir, plus élevé le week-end
    base = 0.35
    morning = 0.9 * np.exp(-((hour - 7.5) ** 2) / 2.0)
    evening = 1.6 * np.exp(-((hour - 20.0) ** 2) / 3.0)
    weekend = np.where(dow >= 5, 0.35, 0.0)
    # Saisonnalité (chauffage/climatisation)
    season = 0.25 * np.cos(2 * np.pi * (idx.dayofyear.values - 15) / 365)
    power = base + morning + evening + weekend + season
    power = np.clip(power + rng.normal(0, 0.12, n), 0.08, None)

    # Quelques pics anormaux (appareil défectueux) : 1 heure à x3
    n_days = n // 1440
    for d in rng.choice(np.arange(n_days), size=max(3, n_days // 120), replace=False):
        h = int(rng.integers(0, 22))
        s = int(d) * 1440 + h * 60
        power[s:s + 60] *= 3.0

    voltage = 240 + rng.normal(0, 2.0, n) - 0.8 * power
    intensity = power * 1000 / voltage
    sub1 = np.clip(rng.normal(0.4, 1.0, n) * (power > 1.5), 0, None)
    sub2 = np.clip(rng.normal(0.5, 1.0, n) * (power > 1.2), 0, None)
    sub3 = np.clip(power * 3.0 + rng.normal(0, 1.0, n), 0, 20)
    reactive = np.clip(0.1 + 0.05 * power + rng.normal(0, 0.03, n), 0, None)

    df = pd.DataFrame({
        "Date": idx.day.astype(str) + "/" + idx.month.astype(str) + "/" + idx.year.astype(str),  # portable (Windows/Linux)
        "Time": idx.strftime("%H:%M:%S"),
        "Global_active_power": np.round(power, 3).astype(str),
        "Global_reactive_power": np.round(reactive, 3).astype(str),
        "Voltage": np.round(voltage, 2).astype(str),
        "Global_intensity": np.round(intensity, 1).astype(str),
        "Sub_metering_1": np.round(sub1, 0).astype(str),
        "Sub_metering_2": np.round(sub2, 0).astype(str),
        "Sub_metering_3": np.round(sub3, 0).astype(str),
    })
    # Valeurs manquantes marquées '?' comme dans le fichier d'origine
    miss = rng.random(n) < 0.012
    df.loc[miss, ["Global_active_power", "Global_reactive_power", "Voltage",
                  "Global_intensity", "Sub_metering_1", "Sub_metering_2",
                  "Sub_metering_3"]] = "?"
    df.to_csv(a.out, sep=";", index=False)
    print(f"{n} lignes écrites dans {a.out}")


if __name__ == "__main__":
    main()
