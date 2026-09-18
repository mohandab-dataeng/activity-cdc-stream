"""
config.py
Centralise l'accès aux variables d'environnement pour tout le projet.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# PostgreSQL
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Redpanda
REDPANDA_BROKERS = os.getenv("REDPANDA_BROKERS", "localhost:19092")
REDPANDA_TOPIC = os.getenv("REDPANDA_TOPIC", "activites_sportives")

# API externes
OPENROUTESERVICE_API_KEY = os.getenv("OPENROUTESERVICE_API_KEY")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# Grafana
GRAFANA_ADMIN_PASSWORD = os.getenv("GRAFANA_ADMIN_PASSWORD")