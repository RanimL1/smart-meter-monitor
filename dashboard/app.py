"""Dashboard Streamlit : surveillance de la consommation électrique d'un foyer.

Lit MongoDB (alimenté par Spark) : hourly_stats, alerts, reference_profile, daily_stats.
4 onglets : temps réel, profil de référence, historique batch, état du pipeline.
"""
import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
DB = os.getenv("MONGO_DB", "smartmeter")
Z = float(os.getenv("Z_THRESHOLD", "3.0"))

# Spark dayofweek : 1 = dimanche ... 7 = samedi
JOURS = {1: "Dimanche", 2: "Lundi", 3: "Mardi", 4: "Mercredi", 5: "Jeudi", 6: "Vendredi", 7: "Samedi"}
ORDRE_JOURS = [2, 3, 4, 5, 6, 7, 1]  # on affiche à partir du lundi
ROUGE, BLEU, GRIS = "#d62728", "#1f77b4", "#7f7f7f"

st.set_page_config(page_title="Smart Meter Monitor", page_icon="⚡", layout="wide")


@st.cache_resource
def get_db():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)[DB]


db = get_db()

# ---------------------------------------------------------------- barre latérale
with st.sidebar:
    st.header("Paramètres")
    n_hours = st.slider("Heures affichées", 24, 24 * 14, 24 * 3, step=24)
    auto = st.checkbox("Rafraîchissement automatique", value=True)
    every = st.slider("Intervalle (secondes)", 1, 10, 3)
    st.caption(f"Seuil d'alerte : |z| ≥ {Z:g}")
    st.divider()
    st.caption("Kafka → Spark Structured Streaming → MongoDB. "
               "Chaque point est la puissance moyenne d'une heure, comparée au comportement "
               "normal calculé en batch sur HDFS.")

st.title("⚡ Surveillance de la consommation électrique")

try:
    db.command("ping")
except Exception as e:  # MongoDB injoignable
    st.error(f"MongoDB injoignable : {e}")
    st.stop()


def load_hourly(limit):
    rows = list(db.hourly_stats.find({}, {"_id": 0}).sort("window_start", -1).limit(limit))
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values("window_start").reset_index(drop=True)


def severity(z):
    if pd.isna(z):
        return "n/a"
    return "Critique" if abs(z) >= 2 * Z else "Alerte" if abs(z) >= Z else "Normal"


def freshness():
    """Âge de la dernière écriture du job streaming et débit sur la dernière minute."""
    st_doc = db.status.find_one({"_id": "streaming"})
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    if not st_doc:
        return None, 0, None
    age = (now - st_doc["last_write"]).total_seconds()
    par_minute = db.hourly_stats.count_documents({"processed_at": {"$gte": now - timedelta(seconds=60)}})
    return age, par_minute, st_doc


def etat_flux(age):
    if age is None:
        return "⚪ Aucune écriture"
    if age <= 15:
        return "🟢 Flux actif"
    if age <= 120:
        return "🟠 Ralenti"
    return "🔴 Arrêté"


def fmt_age(age):
    if age is None:
        return "n/a"
    return f"il y a {int(age)} s" if age < 120 else f"il y a {int(age // 60)} min"


# ---------------------------------------------------------------- onglet 1 : temps réel
def realtime_view():
    df = load_hourly(n_hours)
    if df.empty:
        st.info("Aucune donnée en streaming pour l'instant. Lancez le job Spark Streaming, "
                "puis le producteur.")
        return

    age, par_minute, _ = freshness()
    f1, f2_, f3 = st.columns(3)
    f1.metric("État du flux", etat_flux(age))
    f2_.metric("Dernière écriture (Spark → MongoDB)", fmt_age(age))
    f3.metric("Fenêtres écrites (dernière minute)", par_minute)

    total = db.hourly_stats.count_documents({})
    n_alerts = db.alerts.count_documents({})
    last = df.iloc[-1]
    delta = None if pd.isna(last.get("mean_power")) else last["avg_power"] - last["mean_power"]
    statut = severity(last.get("z_score"))

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Heures analysées", f"{total:,}".replace(",", " "))
    c2.metric("Alertes détectées", n_alerts, f"{100 * n_alerts / max(total, 1):.1f} % des heures",
              delta_color="off")
    c3.metric("Dernière heure (kW)", f'{last["avg_power"]:.2f}',
              None if delta is None else f"{delta:+.2f} vs référence", delta_color="inverse")
    c4.metric("z-score", "n/a" if pd.isna(last.get("z_score")) else f'{last["z_score"]:+.1f}')
    c5.metric("Statut", {"Normal": "🟢 Normal", "Alerte": "🟠 Alerte",
                         "Critique": "🔴 Critique"}.get(statut, "n/a"))

    # Courbe : consommation, bande normale (référence ± Z écarts-types) et alertes en rouge
    haut = df["mean_power"] + Z * df["std_power"]
    bas = (df["mean_power"] - Z * df["std_power"]).clip(lower=0)
    al = df[df["is_alert"] == True]  # noqa: E712
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["window_start"], y=haut, line=dict(width=0),
                             hoverinfo="skip", showlegend=False))
    fig.add_trace(go.Scatter(x=df["window_start"], y=bas, fill="tonexty", line=dict(width=0),
                             fillcolor="rgba(44,160,44,0.15)", name=f"Zone normale (±{Z:g} σ)",
                             hoverinfo="skip"))
    fig.add_trace(go.Scatter(x=df["window_start"], y=df["mean_power"], name="Référence batch",
                             line=dict(color=GRIS, dash="dot")))
    fig.add_trace(go.Scatter(x=df["window_start"], y=df["avg_power"], name="Consommation",
                             line=dict(color=BLEU, width=2)))
    fig.add_trace(go.Scatter(x=al["window_start"], y=al["avg_power"], mode="markers",
                             name="Alerte", marker=dict(color=ROUGE, size=10, symbol="x"),
                             customdata=al[["z_score"]],
                             hovertemplate="%{x}<br>%{y:.2f} kW<br>z = %{customdata[0]:.1f}<extra></extra>"))
    fig.update_layout(template="plotly_white", height=420, margin=dict(l=10, r=10, t=30, b=10),
                      yaxis_title="Puissance moyenne (kW)", hovermode="x unified",
                      legend=dict(orientation="h", y=1.1))
    st.plotly_chart(fig, width="stretch")

    left, right = st.columns([3, 2])
    with left:
        st.subheader("Dernières alertes")
        alerts = list(db.alerts.find({}, {"_id": 0}).sort("window_start", -1).limit(12))
        if alerts:
            a = pd.DataFrame(alerts)
            a["Gravité"] = a["z_score"].map(severity)
            a = a[["window_start", "avg_power", "mean_power", "z_score", "direction", "Gravité"]]
            a.columns = ["Heure", "Consommation (kW)", "Référence (kW)", "z-score", "Type", "Gravité"]
            st.dataframe(a.round(2), width="stretch", hide_index=True)
        else:
            st.success("Aucune alerte pour l'instant.")
    with right:
        st.subheader("Alertes par jour")
        pipeline = [{"$group": {"_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$window_start"}},
                                "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}]
        par_jour = list(db.alerts.aggregate(pipeline))
        if par_jour:
            d = pd.DataFrame(par_jour).rename(columns={"_id": "Jour", "n": "Alertes"})
            f2 = go.Figure(go.Bar(x=d["Jour"], y=d["Alertes"], marker_color=ROUGE))
            f2.update_layout(template="plotly_white", height=300, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(f2, width="stretch")
        else:
            st.caption("Pas encore d'alerte à afficher.")
    st.caption(f"Dernière mise à jour : {datetime.now():%H:%M:%S}")


# ---------------------------------------------------------------- onglet 2 : profil de référence
def reference_view():
    ref = list(db.reference_profile.find({}, {"_id": 0}))
    if not ref:
        st.info("Profil de référence absent : lancez batch_reference.py.")
        return
    r = pd.DataFrame(ref)
    st.subheader("Consommation « normale » par jour de semaine et par heure")
    st.caption("Calculé par Spark (batch) sur l'historique. C'est la référence à laquelle "
               "le streaming compare chaque heure en direct.")
    mean = r.pivot(index="dow", columns="hour", values="mean_power").reindex(ORDRE_JOURS)
    std = r.pivot(index="dow", columns="hour", values="std_power").reindex(ORDRE_JOURS)
    fig = go.Figure(go.Heatmap(z=mean.values, x=[f"{h} h" for h in mean.columns],
                               y=[JOURS[d] for d in mean.index], colorscale="YlOrRd",
                               colorbar=dict(title="kW"),
                               hovertemplate="%{y} %{x}<br>moyenne : %{z:.2f} kW<extra></extra>"))
    fig.update_layout(template="plotly_white", height=380, margin=dict(l=10, r=10, t=10, b=10),
                      yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig, width="stretch")

    jour = st.selectbox("Détail d'un jour", [JOURS[d] for d in ORDRE_JOURS])
    d = [k for k, v in JOURS.items() if v == jour][0]
    m, s = mean.loc[d], std.loc[d]
    f2 = go.Figure()
    f2.add_trace(go.Scatter(x=list(m.index), y=m + Z * s, line=dict(width=0), hoverinfo="skip",
                            showlegend=False))
    f2.add_trace(go.Scatter(x=list(m.index), y=(m - Z * s).clip(lower=0), fill="tonexty",
                            line=dict(width=0), fillcolor="rgba(44,160,44,0.15)",
                            name=f"±{Z:g} σ", hoverinfo="skip"))
    f2.add_trace(go.Scatter(x=list(m.index), y=m, name="Moyenne", line=dict(color=BLEU, width=2)))
    f2.update_layout(template="plotly_white", height=300, margin=dict(l=10, r=10, t=10, b=10),
                     xaxis_title="Heure de la journée", yaxis_title="kW")
    st.plotly_chart(f2, width="stretch")


# ---------------------------------------------------------------- onglet 3 : historique batch
def history_view():
    daily = list(db.daily_stats.find({}, {"_id": 0}).sort("day", 1))
    if not daily:
        st.info("Statistiques batch absentes : lancez batch_reference.py.")
        return
    d = pd.DataFrame(daily)
    d["day"] = pd.to_datetime(d["day"])
    d = d.set_index("day")
    c1, c2, c3 = st.columns(3)
    c1.metric("Jours analysés", f"{len(d):,}".replace(",", " "))
    c2.metric("Consommation moyenne", f'{d["kwh"].mean():.1f} kWh/jour')
    c3.metric("Pic de puissance", f'{d["max_kw"].max():.1f} kW')

    st.subheader("Consommation journalière")
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=d.index, y=d["kwh"], name="kWh/jour",
                             line=dict(color="rgba(31,119,180,0.35)", width=1)))
    fig.add_trace(go.Scatter(x=d.index, y=d["kwh"].rolling(30, min_periods=7).mean(),
                             name="Moyenne mobile 30 j", line=dict(color=BLEU, width=3)))
    fig.update_layout(template="plotly_white", height=340, margin=dict(l=10, r=10, t=10, b=10),
                      yaxis_title="kWh", legend=dict(orientation="h", y=1.1))
    st.plotly_chart(fig, width="stretch")

    st.subheader("Répartition mensuelle par usage")
    m = d[["kwh", "kwh_kitchen", "kwh_laundry", "kwh_heating_ac"]].groupby(d.index.strftime("%Y-%m")).sum()
    m["Autres usages"] = (m["kwh"] - m[["kwh_kitchen", "kwh_laundry", "kwh_heating_ac"]].sum(axis=1)).clip(lower=0)
    noms = {"kwh_kitchen": "Cuisine", "kwh_laundry": "Buanderie",
            "kwh_heating_ac": "Chauffe-eau / clim", "Autres usages": "Autres usages"}
    f2 = go.Figure([go.Bar(x=m.index, y=m[c], name=n) for c, n in noms.items()])
    f2.update_layout(barmode="stack", template="plotly_white", height=340,
                     margin=dict(l=10, r=10, t=10, b=10), yaxis_title="kWh",
                     legend=dict(orientation="h", y=1.1))
    st.plotly_chart(f2, width="stretch")


# ---------------------------------------------------------------- onglet 4 : pipeline
def pipeline_view():
    st.subheader("État du pipeline")
    n_ref = db.reference_profile.count_documents({})
    n_day = db.daily_stats.count_documents({})
    n_h = db.hourly_stats.count_documents({})
    n_al = db.alerts.count_documents({})
    last = db.hourly_stats.find_one({}, {"window_start": 1}, sort=[("window_start", -1)])
    rows = [
        ("Batch : profil de référence", n_ref, "168 attendues (7 jours × 24 h)", n_ref == 168),
        ("Batch : statistiques journalières", n_day, "au moins une ligne", n_day > 0),
        ("Streaming : heures analysées", n_h, "croît pendant le rejeu", n_h > 0),
        ("Streaming : alertes", n_al, "dépend des anomalies", True),
    ]
    t = pd.DataFrame([(("✅" if ok else "⚠️"), nom, n, att) for nom, n, att, ok in rows],
                     columns=["", "Composant", "Documents MongoDB", "Attendu"])
    st.dataframe(t, width="stretch", hide_index=True)
    age, par_minute, _ = freshness()
    st.write(f"**État du flux streaming** : {etat_flux(age)} ({fmt_age(age)}, {par_minute} fenêtres/min)")
    if last:
        st.caption(f"Dernière heure traitée (temps d'événement) : {last['window_start']:%Y-%m-%d %H:%M}")
    st.markdown(
        "**Chaîne de traitement** : fichier brut (HDFS) → Spark batch (nettoyage, profil) → "
        "MongoDB ; producteur → Kafka → Spark Structured Streaming (fenêtres d'une heure, "
        "z-score) → MongoDB → ce dashboard.")


tab1, tab2, tab3, tab4 = st.tabs(["Temps réel", "Profil de référence", "Historique batch", "Pipeline"])
with tab1:
    # Seul cet onglet se rafraîchit automatiquement (pas de rechargement de toute la page)
    st.fragment(run_every=every if auto else None)(realtime_view)()
with tab2:
    reference_view()
with tab3:
    history_view()
with tab4:
    pipeline_view()
