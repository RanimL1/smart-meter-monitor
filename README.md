# Smart Meter Monitor : surveillance de la consommation électrique d'un foyer

Projet Big Data. Plateforme Lambda : le **batch** apprend le comportement normal de consommation, le **streaming** détecte en direct les écarts et alerte.

```
household_power_consumption.txt ─► HDFS (brut) ─► Spark batch ─► Parquet nettoyé ─► profil de référence ─┐
                                                                                                        │ (jointure)
replay_producer.py ─► Kafka (topic power_readings) ─► Spark Structured Streaming ───────────────────────┤
                                                                                                        ▼
                                                                                  MongoDB (hourly_stats, alerts, …) ─► Streamlit
```

## 1. Problème et justification big data

Les compteurs intelligents envoient une mesure toutes les minutes. Un fournisseur gère des millions de compteurs : il doit repérer en continu les surconsommations (appareil défectueux, usage anormal) et comprendre la demande par heure et par jour.

| Caractéristique | Dans ce projet |
|---|---|
| Volume | environ 2 millions de mesures pour un seul foyer (4 ans), multipliées par le nombre de compteurs |
| Vélocité | flux continu, alerte à produire dès qu'une heure est terminée |
| Variété | puissance active et réactive, tension, intensité, trois sous-compteurs |

Données : *Individual Household Electric Power Consumption* (UCI), fichier CSV, séparateur `;`, `?` = valeur manquante. Plan B : générateur de données synthétiques au même format.

## 2. Technologies et justification

| Technologie | Rôle | Pourquoi | Limite |
|---|---|---|---|
| HDFS + Parquet | Data lake : fichier brut et données nettoyées | Stockage distribué et répliqué, Parquet en colonnes, lecture sélective par année | Mauvais pour de petits fichiers |
| Spark (batch) | Nettoyage, profil de référence, bilans journaliers | Calcul distribué, SQL et DataFrame | Résultats non instantanés |
| Kafka (mode KRaft) | Ingestion du flux de mesures | Découple le producteur de Spark, conserve les messages, permet le rejeu | Plus de composants à déployer |
| Spark Structured Streaming | Fenêtres d'une heure, z-score, alertes | Même API que le batch : un seul moteur à maîtriser | Latence de micro-lot |
| MongoDB | Résultats : heures analysées, alertes, profils | Documents JSON flexibles, lecture rapide par date | Pas de jointures riches |
| Streamlit | Dashboard | Rapide à mettre en place | Interactivité limitée |
| Docker Compose | Déploiement reproductible | Même environnement pour les trois membres | Consomme de la RAM |

**Pourquoi une architecture Lambda ?** Le batch lit tout l'historique pour définir « normal » (moyenne et écart-type par jour de semaine et par heure). Le streaming ne peut pas recalculer cet historique à chaque message : il compare l'heure en cours à cette référence déjà calculée.

## 3. Méthode de détection

1. **Batch** : pour chaque heure de l'historique antérieur à `REF_END` (par défaut 2010-01-01), on calcule la puissance moyenne ; puis, pour chaque (jour de semaine, heure), la moyenne et l'écart-type de ces moyennes horaires.
2. **Streaming** : fenêtre fixe d'1 heure sur le temps d'événement, watermark de 10 minutes, fenêtre ignorée si elle contient moins de 30 mesures.
3. **Score** : `z = (moyenne_heure − moyenne_référence) / écart_type_référence`. **Alerte** si `|z| ≥ 3`.
4. **Pas de fuite de données** : la référence utilise le passé, le rejeu commence à `REF_END`.

Limites à connaître : un seul foyer, la saisonnalité n'est pas modélisée (le profil moyenne toute l'année), un z-score suppose des moyennes horaires à peu près symétriques.

## 4. Structure du dépôt

```
docker-compose.yml        
hadoop.config             
data/                     
scripts/
  download_data.sh        
  generate_synthetic_data.py   
producer/
  replay_producer.py      
spark/
  common.py               
  prepare_data.py         
  batch_reference.py      
  streaming_job.py        
dashboard/app.py          
docs/journal_prompts.md   
```

## 5. Lancement pas à pas

### 5.1 Données

```bash
bash scripts/download_data.sh                      
python scripts/generate_synthetic_data.py          
```

### 5.2 Infrastructure

```bash
docker compose up -d --build
docker compose ps                                  
```
Interfaces : HDFS http://localhost:9870, Spark http://localhost:8080, Dashboard http://localhost:8501.

### 5.3 Chargement dans HDFS

```bash
docker compose exec namenode hdfs dfs -mkdir -p /data/raw
docker compose exec namenode hdfs dfs -put -f /data/household_power_consumption.txt /data/raw/
docker compose exec namenode hdfs dfs -ls /data/raw
```

### 5.4 Batch

```bash
SUBMIT="docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy"
$SUBMIT /app/spark/prepare_data.py    
$SUBMIT /app/spark/batch_reference.py    
```

### 5.5 Streaming (terminal dédié, reste ouvert)

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /app/spark/streaming_job.py
```

### 5.6 Producteur (autre terminal)

```bash
docker compose run --rm producer --rate 300 --anomaly-prob 0.02
```
`--rate` = mesures par seconde (300 mesures/s ≈ 5 heures de données par seconde). `--anomaly-prob 0.02` gonfle environ 2 % des heures d'un facteur 3 pour provoquer des alertes pendant la démo.

Ouvrez ensuite le dashboard : courbe, référence, seuil et tableau d'alertes se remplissent.

### 5.7 Test sans Docker (développement local)

```bash
pip install pyspark==3.5.1 pymongo
export BASE_PATH=file:///tmp/sm && mkdir -p /tmp/sm/data/raw
cp data/household_power_consumption.txt /tmp/sm/data/raw/
cd spark && python prepare_data.py && cd ..
python producer/replay_producer.py --dry-run --file data/household_power_consumption.txt --limit 5
```

## 6. Dépannage

| Symptôme | Piste |
|---|---|
| `Failed to find data source: kafka` | `--packages` oublié, ou version différente de Spark (3.5.1) et de Scala (2.12) |
| `Connection refused` vers `namenode:8020` | NameNode pas encore prêt : `docker compose logs namenode` |
| Aucune alerte | Baisser `Z_THRESHOLD` (variable d'environnement) ou utiliser `--anomaly-prob` |
| Streaming sans sortie | Une fenêtre n'est émise qu'après la fin de l'heure + watermark ; augmentez `--rate` |
| Manque de mémoire | Réduire `SPARK_WORKER_MEMORY`, fermer les autres applications |
| Rejouer depuis le début | Supprimer le checkpoint : `hdfs dfs -rm -r /checkpoints/streaming_job` |





