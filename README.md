# Pipeline de données météorologiques en temps réel

Projet de démonstration d'un pipeline de données météo avec Kafka, Spark Structured
Streaming, MinIO, MLflow, PostgreSQL, Prometheus et Grafana.

Le projet génère des événements météo simulés afin de fonctionner sans clé API.
Le producteur peut ensuite être remplacé par un connecteur Open-Meteo ou une autre
source, à condition de conserver le contrat de données défini dans
`src/common/schema.py`.

## Architecture

```text
                         ┌─────────────────────┐
                         │ weather-producer    │
                         │ événements + /metrics│
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │ Kafka               │
                         │ weather.raw         │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │ Spark Streaming     │
                         │ validation qualité  │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │ MinIO               │
                         │ weather/clean/*.parquet
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │ Trainer             │
                         │ Random Forest       │
                         │ XGBoost             │
                         │ LSTM optionnel      │
                         └──────────┬──────────┘
                                    │
                         ┌──────────┴──────────┐
                         ▼                     ▼
                  PostgreSQL                 MinIO
                  métadonnées MLflow         modèles/artefacts

  Prometheus ──► métriques exposées ──► Grafana
```

### Rôle des stockages

| Composant | Utilisation |
|---|---|
| Kafka | Transport des événements météo |
| Spark | Lecture Kafka, validation et écriture des lots |
| MinIO | Données Parquet météo et artefacts MLflow |
| PostgreSQL | Expériences, runs, paramètres et métriques MLflow |
| MLflow | Suivi des entraînements et comparaison des modèles |
| Prometheus | Collecte des métriques exposées par les services |
| Grafana | Visualisation des métriques Prometheus |

MLflow et PostgreSQL sont deux conteneurs distincts. MLflow utilise PostgreSQL
comme backend de suivi et MinIO comme stockage d'artefacts.

## Prérequis

- Docker Desktop démarré ;
- Docker Compose v2 ;
- au moins 8 Go de mémoire attribuée à Docker Desktop ;
- Windows PowerShell, Linux ou macOS.

## Démarrage avec Docker

Depuis la racine du projet :

```powershell
cd C:\Users\Marye\Desktop\meteo
docker compose up --build -d
```

Vérifier l'état des services :

```powershell
docker compose ps
```

Les services attendus sont :

```text
kafka
kafka-init
minio
minio-init
mlflow-db
mlflow
weather-producer
spark-streaming
trainer
prometheus
grafana
kafka-ui
```

Le service `kafka-init` est un service d'initialisation : il se termine
normalement après la création des topics. Les autres services restent actifs.

## Interfaces

| Interface | URL | Identifiants |
|---|---|---|
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| MLflow | http://localhost:5000 | aucun par défaut |
| Grafana | http://localhost:3000 | `admin` / `admin` |
| Prometheus | http://localhost:9090 | aucun |
| Kafka UI | http://localhost:8080 | aucun |

## Flux de données

### 1. Producteur météo

Le producteur génère un événement toutes les deux secondes et l'envoie dans
le topic Kafka `weather.raw`.

```powershell
docker compose logs -f weather-producer
```

Il expose aussi ses métriques sur :

```text
http://localhost:8000/metrics
```

Les métriques actuellement exposées sont notamment :

```text
weather_events_produced_total
weather_last_temperature_c
```

### 2. Kafka

Les topics sont créés automatiquement par `kafka-init` :

- `weather.raw` : événements entrants ;
- `weather.processed` : topic réservé aux extensions futures ;
- `weather.dlq` : topic réservé aux erreurs/quarantaines futures.

Lister les topics :

```powershell
docker compose exec kafka `
  /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server localhost:29092 --list
```

### 3. Spark Structured Streaming

Le job lit `weather.raw`, valide chaque micro-lot et écrit les événements
valides dans MinIO au format Parquet :

```text
s3://weather/clean/
```

Voir les traitements :

```powershell
docker compose logs -f spark-streaming
```

Exemple de sortie attendue :

```text
processed=1 valid=1 invalid=0
```

La validation est implémentée dans `src/quality/expectations.py` et s'appuie
sur le schéma partagé de `src/common/schema.py`.

### 4. Entraînement

Le trainer lit les fichiers Parquet présents dans MinIO. Il crée une cible
correspondant à la température de l'événement suivant, puis compare :

- Random Forest ;
- XGBoost ;
- LSTM si TensorFlow est installé.

Le trainer fonctionne en boucle avec un nouvel entraînement toutes les cinq
minutes :

```powershell
docker compose logs -f trainer
```

Déclencher un entraînement ponctuel :

```powershell
docker compose run --rm trainer python -m src.trainer.train
```

Les métriques enregistrées sont :

- `rmse` ;
- `mae` ;
- `r2`.

Le modèle ayant le meilleur RMSE est affiché dans les logs, par exemple :

```text
champion=xgboost rmse=0.73
```

## MLflow, PostgreSQL et MinIO

MLflow sépare les métadonnées et les artefacts :

```text
PostgreSQL :
  expériences, runs, métriques, paramètres et tags MLflow

MinIO :
  model.pkl, MLmodel, requirements.txt, python_env.yaml
```

Les artefacts MLflow sont stockés dans le bucket :

```text
s3://mlflow/
```

Les données météo sont stockées dans :

```text
s3://weather/clean/
```

Consulter les tables MLflow dans PostgreSQL :

```powershell
docker compose exec mlflow-db `
  psql -U mlflow -d mlflow -c "\dt"
```

Consulter les dernières métriques :

```powershell
docker compose exec mlflow-db `
  psql -U mlflow -d mlflow `
  -c "SELECT key, value, timestamp FROM metrics ORDER BY timestamp DESC LIMIT 10;"
```

## Prometheus et Grafana

Prometheus est configuré dans
`monitoring/prometheus.yml`. Il utilise le modèle Pull : toutes les
15 secondes, il interroge les endpoints `/metrics` configurés.

Dans la configuration actuelle, Prometheus interroge :

- `prometheus:9090` ;
- `weather-producer:8000`.

Il ne lit pas directement Kafka, MinIO, PostgreSQL ou MLflow. Pour surveiller
ces composants, il faut ajouter leurs exporters ou exposer des métriques
personnalisées.

Grafana utilise Prometheus comme source de données et charge le dashboard
depuis :

```text
monitoring/grafana/dashboards/weather.json
```

La configuration actuelle permet de visualiser principalement :

- la disponibilité des jobs ;
- le nombre d'événements produits ;
- la dernière température exposée par le producteur.

Les métriques détaillées des modèles restent consultables dans MLflow. Une
source PostgreSQL peut être ajoutée à Grafana ultérieurement pour afficher
RMSE, MAE et R² directement dans des panneaux SQL.

## Commandes utiles

```powershell
# Voir tous les logs
docker compose logs -f

# Voir un service précis
docker compose logs -f spark-streaming
docker compose logs -f trainer

# Vérifier les conteneurs
docker compose ps

# Redémarrer un service
docker compose restart weather-producer

# Arrêter sans supprimer les données
docker compose down

# Arrêter et supprimer les volumes de démonstration
docker compose down -v
```

## Tests locaux

Pour installer les dépendances dans un environnement Python :

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Lancer les tests :

```powershell
python -m pytest
```

Générer quelques événements localement :

```powershell
python -m src.producer.weather_producer --count 20 --interval 0.2
```

Pour exécuter Spark hors Docker, il faut installer Spark et les connecteurs
Kafka/S3 compatibles avec la version de Spark utilisée par le projet.

## Structure du projet

```text
.
├── docker/
│   ├── Dockerfile.app
│   ├── Dockerfile.mlflow
│   └── Dockerfile.spark
├── monitoring/
│   ├── prometheus.yml
│   └── grafana/
├── src/
│   ├── common/schema.py
│   ├── producer/weather_producer.py
│   ├── quality/expectations.py
│   ├── streaming/spark_streaming.py
│   └── trainer/train.py
├── tests/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Arrêt et persistance

`docker compose down` arrête les conteneurs sans supprimer les données.
Les données sont conservées dans les volumes Docker :

- `minio-data` ;
- `mlflow-db`.

`docker compose down -v` supprime ces volumes et réinitialise les données
MinIO et PostgreSQL.
