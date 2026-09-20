# activity-cdc-stream

Real-time CDC pipeline: PostgreSQL → Redpanda → PySpark Structured Streaming → Delta Lake. Includes Great Expectations data quality checks, Grafana/Prometheus monitoring, and Metabase BI dashboards. Built for a fictional employee wellness incentive program (POC).

## Contexte

POC pour Sport Data Solution : un système de récompenses pour les salariés pratiquant une activité physique régulière.

Deux avantages sont testés :

- **Prime sportive** : 5 % du salaire brut annuel, pour les salariés venant au bureau en mode actif (marche, course, vélo, trottinette), sous réserve que la distance domicile-travail soit cohérente avec le mode déclaré.
- **Jours bien-être** : 5 jours accordés aux salariés ayant pratiqué au moins 15 activités physiques sur les 12 derniers mois, en dehors du travail.

Le POC vise à valider la faisabilité technique, déterminer les données à collecter, et estimer l'impact financier de ces avantages pour l'entreprise.

## Architecture

```
Strava-like generation (Python) → PostgreSQL
        ↓ (Debezium CDC)
    Redpanda (topic)
        ↓                              ↓
  Spark job 1 (bronze,           Python (Slack)
  données brutes, streaming)
        ↓
  Spark job 2 (silver, enrichi avec
  référentiel entreprise + référentiel sportif)
        ↓
  Delta Lake (silver) → compute_kpis.py → PostgreSQL → Metabase
```

Le pipeline suit une architecture medallion (bronze / silver) : le premier job Spark écrit les données brutes telles qu'elles arrivent du topic, sans transformation ni enrichissement — cohérent avec la pratique standard de l'industrie, qui recommande de garder une couche brute rejouable en cas d'erreur en aval. Le second job lit cette couche bronze, l'enrichit avec les référentiels (entreprise et sportif), et écrit le résultat final en couche silver, prête pour le calcul des KPI.

## Stack technique et justifications

| Composant | Outil | Justification |
|---|---|---|
| Base de données source | PostgreSQL | Schéma relationnel fixe et connu à l'avance, données fortement relationnelles (salarié ↔ activités ↔ référentiels), besoin de cohérence transactionnelle sur des données RH sensibles — trois critères qui pointent vers le relationnel plutôt que le NoSQL |
| CDC | Debezium (Kafka Connect) | Redpanda Connect (l'alternative plus légère, sans JVM) nécessite une licence Enterprise même en version d'essai limitée dans le temps ; Debezium reste gratuit, open source, et sans limite de durée |
| Broker de streaming | Redpanda | Compatible avec le protocole Kafka, plus léger à opérer que Kafka classique |
| Traitement | PySpark Structured Streaming | Standard actuel recommandé pour le streaming (Spark Streaming/DStreams est considéré comme obsolète), accessible en Python |
| Stockage analytique | Delta Lake | Transactions ACID, time travel natif — répond directement à l'exigence de pouvoir relancer l'historique des indicateurs si une source change (ex. taux de prime) |
| Qualité des données | Great Expectations | Vocabulaire standard de l'industrie pour exprimer des règles de cohérence (distances non négatives, dates valides), avec documentation automatique des règles |
| Géocodage (adresse → coordonnées) | Nominatim (OpenStreetMap) | Gratuit, sans quota journalier bas contrairement au géocodage d'OpenRouteService qui a présenté des limitations de débit peu documentées |
| Distance routière domicile-travail | OSRM (auto-hébergé) | Gratuit et illimité une fois les données téléchargées, aucune dépendance à un service tiers avec quota ou clé API |
| Monitoring du pipeline | Prometheus + Grafana | Exigence explicite de la note de cadrage ; métriques natives Redpanda (débit, volumétrie) exposées nativement sans instrumentation supplémentaire |
| Restitution BI | Metabase | Équivalent open source à Power BI ; dashboards avec auto-refresh, persistance via volume Docker |

## Structure du projet

```
activity-cdc-stream/
├── main.py                    # Orchestration du pipeline batch
├── docker-compose.yml
├── pyproject.toml
├── .env.example
├── config/
│   ├── prometheus/
│   │   └── prometheus.yml
│   ├── grafana/
│   │   ├── dashboards/
│   │   └── provisioning/
│   └── redpanda/
│       └── redpanda-dashboard.json
├── data/
│   ├── raw/                   # Fichiers RH sources (non versionnés)
│   ├── osrm/                  # Données de routage OSRM (non versionnées)
│   ├── bronze/                # Delta Lake, données brutes (non versionné)
│   ├── silver/                # Delta Lake, données enrichies (non versionné)
│   └── checkpoints/           # Checkpoints Spark Structured Streaming (non versionné)
├── src/
│   ├── config.py               # Centralisation des variables d'environnement
│   ├── ingestion/
│   │   ├── models.py            # Modèles SQLAlchemy
│   │   ├── init_database.py
│   │   ├── load_rh_data.py
│   │   ├── generate_strava_data.py
│   │   ├── populate_referentiel_sportif.py
│   │   └── simulate_live_activities.py
│   ├── validation/
│   │   └── validate_declarations.py
│   ├── streaming/
│   │   ├── setup_cdc_connector.py
│   │   ├── spark_transform_enrich.py
│   │   └── spark_clean_finalize.py
│   ├── quality/
│   │   └── run_data_quality_checks.py
│   ├── notifications/
│   │   └── generate_slack_messages.py
│   └── analytics/
│       └── compute_kpis.py
├── scripts/
│   ├── run_spark_transform.sh
│   └── run_spark_clean.sh
└── tests/
```

## Prérequis

- Docker et Docker Compose
- Python 3.13 ou supérieur
- [uv](https://docs.astral.sh/uv/) comme gestionnaire de paquets et d'environnement
- Un webhook Slack (pour les notifications d'activité)
- ~6 Go d'espace disque libre (données OSM pour OSRM)

## Installation

### 1. Cloner et préparer l'environnement

```bash
git clone <repo>
cd activity-cdc-stream
cp .env.example .env
```

Éditer `.env` et renseigner :

```
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=sportdata
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin

REDPANDA_BROKERS=localhost:19092
REDPANDA_TOPIC=activites_sportives

SLACK_WEBHOOK_URL=<votre webhook Slack>

GRAFANA_ADMIN_PASSWORD=admin
```

### 2. Installer les dépendances Python

```bash
uv sync
```

### 3. Démarrer l'infrastructure Docker

```bash
docker compose up -d postgres redpanda redpanda-console prometheus grafana metabase kafka-connect spark
```

Vérifier que tous les services sont opérationnels :

```bash
docker compose ps
```

Tous les services doivent afficher un statut `Up (healthy)`.

### 4. Préparer les données de routage OSRM

Cette étape télécharge et prétraite les données routières de la France (environ 5 Go, opération à ne faire qu'une seule fois) :

```bash
mkdir -p data/osrm
cd data/osrm
wget https://download.geofabrik.de/europe/france-latest.osm.pbf
cd ../..

docker run -t -v "$(pwd)/data/osrm:/data" osrm/osrm-backend osrm-extract -p /opt/car.lua /data/france-latest.osm.pbf
docker run -t -v "$(pwd)/data/osrm:/data" osrm/osrm-backend osrm-partition /data/france-latest.osrm
docker run -t -v "$(pwd)/data/osrm:/data" osrm/osrm-backend osrm-customize /data/france-latest.osrm

docker compose up -d osrm
```

### 5. Placer les fichiers RH sources

Déposer les fichiers Excel fournis par les RH dans `data/raw/` (noms contenant `RH` et `Sportive`, le chargement les retrouve automatiquement par motif).

## Lancer le pipeline batch complet

```bash
uv run python main.py
```

Ce script exécute dans l'ordre : initialisation de la base, chargement des données RH, peuplement du référentiel sportif, validation des déclarations de déplacement, configuration du connecteur CDC, génération de l'historique d'activités, et calcul des KPI.

## Lancer les processus de streaming

Ces processus sont des tâches longues, à lancer chacun dans un terminal séparé, en parallèle du pipeline batch :

```bash
# Terminal 1 — premier job Spark (bronze)
./scripts/run_spark_transform.sh

# Terminal 2 — second job Spark (silver, enrichissement)
./scripts/run_spark_clean.sh

# Terminal 3 — notifications Slack en temps réel
uv run python src/notifications/generate_slack_messages.py
```

## Démonstration en direct

Pour simuler un flux d'activités en continu (utile pour la démonstration) :

```bash
uv run python src/ingestion/simulate_live_activities.py
```

Ce script insère une nouvelle activité aléatoire toutes les 30 secondes, ce qui déclenche en cascade : capture par Debezium, publication dans Redpanda, traitement par les deux jobs Spark, et envoi d'une notification Slack — visible en temps réel sur les interfaces suivantes.

## Interfaces disponibles

| Service | URL | Usage |
|---|---|---|
| Redpanda Console | http://localhost:8080 | Suivi des topics, messages, consumer groups |
| Prometheus | http://localhost:9090 | Requêtes brutes sur les métriques du pipeline |
| Grafana | http://localhost:3000 | Dashboards de monitoring (volumétrie, débit) |
| Metabase | http://localhost:3001 | Restitution des KPI métier |
| Spark UI (job actif) | http://localhost:4040 | Suivi détaillé d'un job Spark en cours d'exécution |
| Kafka Connect API | http://localhost:8083 | Statut du connecteur Debezium |

## Points d'architecture à connaître

**CDC et snapshot initial.** Debezium capture les changements sur trois tables (`salaries`, `referentiel_entreprise`, `activites_sportives`), avec un snapshot complet effectué au démarrage du connecteur. Ce snapshot capture l'état existant de ces tables au moment où le connecteur démarre ; les changements ultérieurs sont ensuite capturés en continu.

**Distinction bronze/silver.** Le premier job Spark n'effectue aucun enrichissement : il se contente de lire le topic et d'écrire les données telles quelles en couche bronze. C'est le second job qui joint les référentiels (entreprise et sportif) et produit la couche silver, prête pour l'analyse. Cette séparation permet de rejouer l'enrichissement sans avoir à retraiter le flux Kafka depuis son origine si un référentiel est corrigé.

**Filtrage du snapshot côté notifications.** Le consumer Slack ignore explicitement les messages issus du snapshot initial du CDC (`source.snapshot` différent de `"true"` ou `"last"`), pour éviter d'envoyer une notification pour chaque ligne de l'historique au moment du démarrage du connecteur. Seules les activités réellement nouvelles déclenchent une notification.

**Référentiels statiques dans les jobs Spark.** Les tables de référence (`referentiel_entreprise`, `referentiel_sportif`, `salaries`) sont lues en JDBC classique (`spark.read`, pas `readStream`), ce qui signifie qu'elles sont chargées une fois au démarrage du job et ne se rafraîchissent pas automatiquement en cours d'exécution. Pour un POC dont ces référentiels changent rarement, ce choix est acceptable ; une évolution possible en production serait de charger ces référentiels en Delta Lake plutôt qu'en lecture JDBC directe, pour bénéficier d'un rafraîchissement à chaque micro-lot.

**Validation des distances domicile-travail.** Les règles suivantes sont appliquées, conformément à la note de cadrage :
- Marche ou course à pied : distance maximale de 15 km
- Vélo, trottinette ou autres modes actifs : distance maximale de 25 km

Toute déclaration dépassant ce seuil pour le mode annoncé est signalée comme anomalie dans la table `validations_deplacement`, consultable depuis Metabase.

## Tests de qualité des données

```bash
uv run python src/quality/run_data_quality_checks.py
```

Ce script applique, via Great Expectations, les règles suivantes sur la couche bronze :
- absence de valeurs nulles sur les colonnes critiques (identifiant salarié, dates, type d'activité)
- distances non négatives
- cohérence entre la date de début et la date de fin d'une activité

## Limites connues et pistes d'évolution

- Le calcul des KPI (`compute_kpis.py`) est un script batch à relancer manuellement après une mise à jour des données ; une orchestration par Dagster ou Airflow permettrait de planifier ce recalcul et d'en assurer la reprise automatique en cas d'échec.
- Les référentiels ne bénéficient pas d'un CDC dédié, contrairement aux activités sportives ; ce choix a été fait car la note de cadrage ne demande pas de capture en temps réel pour les référentiels eux-mêmes, mais qu'une synchronisation régulière pourrait être ajoutée si leur fréquence de mise à jour l'exigeait.
- L'intégration directe avec l'API Strava, mentionnée comme cible finale dans la note de cadrage, n'est pas implémentée dans ce POC ; les données sont simulées de manière cohérente avec les profils RH réels.