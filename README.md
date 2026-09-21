# activity-cdc-stream

Real-time CDC pipeline: PostgreSQL → Redpanda → PySpark Structured Streaming → Delta Lake. Includes Great Expectations data quality checks, Grafana/Prometheus monitoring, Metabase BI dashboards, and Kestra orchestration. Built for a fictional employee wellness incentive program (POC).

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

L'ensemble de ce pipeline (étapes batch, démarrage des jobs Spark, notifier Slack) est orchestré par **Kestra**, qui remplace le lancement manuel de chaque script dans des terminaux séparés. Un second flow Kestra recalcule périodiquement les KPI pour que la table `kpis_salaries` (lue par Metabase) reste à jour au fil du streaming. Voir la section [Orchestration avec Kestra](#orchestration-avec-kestra).

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
| Orchestration | Kestra | Open source, pilote Docker nativement (exécution des tâches Python dans des conteneurs isolés, démarrage des jobs Spark et du consumer Slack), UI de suivi des exécutions, planification par cron pour le recalcul périodique des KPI |

## Structure du projet

```
activity-cdc-stream/
├── main.py                    # Orchestration du pipeline batch (alternative locale à Kestra)
├── Dockerfile                  # Image utilisée par Kestra pour exécuter les tâches Python
├── docker-compose.yaml
├── pyproject.toml
├── .env.exemple
├── kestra/
│   └── flows/
│       ├── activity-cdc-pipeline.yaml   # Flow principal : batch + streaming + KPI
│       └── refresh-kpis.yaml            # Flow planifié : recalcul périodique des KPI
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
- ~6 Go d'espace disque libre (données OSM pour OSRM) garder la capacité de calcul en local

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

## Orchestration avec Kestra

Le pipeline peut être piloté entièrement depuis Kestra (`docker compose up -d kestra`, interface sur `http://localhost:8085`), plutôt que de lancer chaque script manuellement. C'est l'approche recommandée : elle enchaîne le batch, démarre les jobs Spark et le consumer Slack, et calcule les KPI en une seule exécution suivie depuis l'UI.

### 1. Builder l'image utilisée par les tâches Python

Les tâches Python du flow s'exécutent dans des conteneurs Docker isolés (et non dans le conteneur Kestra lui-même, qui n'a pas les dépendances du projet). Il faut construire cette image une première fois, puis à chaque modification du code source :

```bash
docker build -t activity-cdc-stream-pipeline:latest .
```

### 2. Renseigner le KV Store

Les identifiants sensibles (mot de passe PostgreSQL, webhook Slack) ne sont jamais écrits en dur dans les flows : ils sont lus depuis le KV Store de Kestra, namespace `sportdata`. À renseigner une fois, via l'UI (**Namespaces → sportdata → KV Store → Create**) :

| Clé | Exemple de valeur |
|---|---|
| `POSTGRES_DB` | `sportdata` |
| `POSTGRES_USER` | `admin` |
| `POSTGRES_PASSWORD` | `admin` |
| `REDPANDA_TOPIC` | `activites_sportives` |
| `SLACK_WEBHOOK_URL` | `<votre webhook Slack>` |

### 3. Importer les flows

Les définitions de flows sont versionnées dans `kestra/flows/` mais ne sont pas rechargées automatiquement par Kestra après le premier démarrage. Les importer manuellement dans l'UI (**Flows → Create**, en collant le contenu du fichier) :

- `activity-cdc-pipeline.yaml` — pipeline complet (batch, jobs Spark, notifier Slack, calcul des KPI)
- `refresh-kpis.yaml` — recalcul périodique des KPI (déclenché toutes les 2 minutes par un trigger `Schedule`)

### 4. Exécuter le pipeline

Depuis l'UI, ouvrir `sportdata / activity-cdc-pipeline` et cliquer sur **Execute**. Les inputs disponibles :

| Input | Défaut | Usage |
|---|---|---|
| `host_project_dir` | chemin absolu du projet | Chemin du dépôt sur l'hôte Docker (pas dans le conteneur Kestra) — sert à monter `data/` dans les conteneurs éphémères créés par les tâches |
| `pipeline_image` | `activity-cdc-stream-pipeline:latest` | Image buildée à l'étape 1 |
| `run_quality_checks` | `true` | Lance les checks Great Expectations après le batch |
| `run_live_simulation` | `false` | Démarre `simulate_live_activities.py` en arrière-plan (démo en direct) |

Le flow enchaîne : initialisation de la base, chargement RH, référentiel sportif, validation des déplacements, connecteur CDC, génération de l'historique d'activités, puis démarre en arrière-plan les jobs Spark bronze/silver et le consumer Slack, avant de calculer les KPI.

### Points d'attention

- **Processus longs, pas de tâches bloquantes.** Les jobs Spark, le consumer Slack et la simulation live sont des boucles infinies : le flow les démarre en arrière-plan (`docker exec -d` / `docker run -d`) plutôt que de les exécuter comme des tâches Kestra classiques, qui ne se termineraient jamais.
- **Pas d'accumulation entre exécutions.** Avant de démarrer un nouveau consumer Slack ou une nouvelle simulation live, le flow arrête et supprime tout conteneur du même type restant d'une exécution précédente — sans cela, plusieurs consumers coexisteraient sur le même groupe Kafka et fausseraient le lag affiché dans Grafana.
- **Recalcul des KPI.** `compute_kpis.py` reste un script batch : il ne se déclenche pas tout seul quand une nouvelle activité arrive. Le flow `refresh-kpis` comble ce manque en le relançant automatiquement toutes les 2 minutes, pour que Metabase reflète les données à jour.

## Lancer le pipeline manuellement (sans Kestra)

Pour un lancement local, sans passer par Kestra :

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

Ce script insère une nouvelle activité aléatoire toutes les 60 secondes, ce qui déclenche en cascade : capture par Debezium, publication dans Redpanda, traitement par les deux jobs Spark, et envoi d'une notification Slack — visible en temps réel sur les interfaces suivantes.

Cette même simulation peut aussi être démarrée depuis Kestra, en exécutant `activity-cdc-pipeline` avec l'input `run_live_simulation` à `true` (voir [Orchestration avec Kestra](#orchestration-avec-kestra)).

## Interfaces disponibles

| Service | URL | Usage |
|---|---|---|
| Kestra | http://localhost:8085 | Orchestration : exécution et suivi des flows, KV Store |
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

- Le calcul des KPI (`compute_kpis.py`) reste un script batch, sans mécanisme de streaming propre ; le flow Kestra `refresh-kpis` compense en le relançant toutes les 2 minutes, mais une reprise automatique en cas d'échec (retry, alerting) reste à ajouter.
- Le lag du consumer Slack (visible dans Grafana) peut sembler élevé après une regénération massive de l'historique d'activités (`generer_activites`), car chaque message est espacé d'un court délai côté consumer avant l'envoi Slack ; il redescend progressivement sans action requise.
- Les référentiels ne bénéficient pas d'un CDC dédié, contrairement aux activités sportives ; ce choix a été fait car la note de cadrage ne demande pas de capture en temps réel pour les référentiels eux-mêmes, mais qu'une synchronisation régulière pourrait être ajoutée si leur fréquence de mise à jour l'exigeait.
- L'intégration directe avec l'API Strava, mentionnée comme cible finale dans la note de cadrage, n'est pas implémentée dans ce POC ; les données sont simulées de manière cohérente avec les profils RH réels.