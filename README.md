# Pipeline de données météorologiques temps réel

Pipeline de démonstration complet basé sur Kafka, Spark Structured Streaming, Great Expectations, MinIO, MLflow et Grafana.

## Architecture

```text
weather-producer --> Kafka (weather.raw) --> Spark Streaming
                                             |--> Great Expectations
                                             |--> MinIO /weather/clean/*.parquet
                                             |--> Kafka (weather.processed)
                                             |--> MinIO /weather/features/*.parquet
                                                        |
                                                        '--> trainer
                                                             |--> XGBoost
                                                             |--> Random Forest
                                                             '--> LSTM
                                                                  '--> MLflow

MinIO metrics / Spark metrics --> Prometheus --> Grafana
```

Le projet utilise des données simulées afin de fonctionner sans clé API. Le producteur peut être remplacé par un adaptateur vers Open-Meteo ou une autre API en conservant le schéma Kafka.

## Démarrage rapide

Pré-requis : Docker Desktop avec au moins 8 Go de mémoire et Docker Compose v2.

```powershell
docker compose up --build -d
docker compose logs -f weather-producer
```

Interfaces :

- Kafka UI : http://localhost:8080
- MinIO : http://localhost:9001 (`minioadmin` / `minioadmin`)
- MLflow : http://localhost:5000
- Grafana : http://localhost:3000 (`admin` / `admin`)
- Prometheus : http://localhost:9090

Le job Spark démarre automatiquement, crée les topics, valide et écrit les données. Le trainer est relancé toutes les 5 minutes lorsque suffisamment de données sont disponibles.

## Commandes utiles

```powershell
# Voir les logs du traitement
docker compose logs -f spark-streaming

# Déclencher manuellement un entraînement
docker compose run --rm trainer

# Arrêter et supprimer les volumes de démonstration
docker compose down -v
```

## Contrats de données

Chaque événement `weather.raw` contient `event_id`, `timestamp`, `station_id`, `latitude`, `longitude`, `temperature_c`, `humidity_pct`, `pressure_hpa`, `wind_speed_kmh` et `precipitation_mm`.

Les contrôles Great Expectations vérifient :

- présence et type des colonnes ;
- température entre -80 et 60 °C ;
- humidité entre 0 et 100 % ;
- pression et vitesse du vent positives ;
- unicité de `event_id` dans le micro-lot.

Les lignes invalides sont envoyées vers `weather.dlq` avec le détail des erreurs, tandis que les lignes valides sont écrites en Parquet.

## Modèles

La cible est la température à l'horizon suivant (`temperature_next_c`). Le script `src/trainer/train.py` compare :

- Random Forest ;
- XGBoost ;
- LSTM (si TensorFlow est installé ; il est optionnel pour démarrer la plateforme).

Les métriques RMSE, MAE et R² sont journalisées dans MLflow. Le meilleur modèle selon RMSE est affiché dans les logs et peut être promu dans le registre MLflow. Le producteur expose aussi `weather_events_produced_total` et `weather_last_temperature_c` pour Grafana/Prometheus.

## Développement local

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m src.producer.weather_producer --count 20 --interval 0.2
python -m pytest
```

Pour exécuter Spark hors Docker, utilisez le fichier `src/streaming/spark_streaming.py` avec un Spark disposant des connecteurs Kafka et S3A correspondants.
