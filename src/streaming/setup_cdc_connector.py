"""
setup_cdc_connector.py
Enregistre (ou met à jour) le connecteur Debezium PostgreSQL auprès de
Kafka Connect, pour capturer les changements sur salaries,
referentiel_entreprise et activites_sportives, et les publier dans Redpanda.
"""

import os

import requests
import time

KAFKA_CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083")

CONNECTOR_CONFIG = {
    "name": "postgres-activites-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": os.getenv("POSTGRES_USER"),
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "database.dbname": os.getenv("POSTGRES_DB", "sportdata"),
        "topic.prefix": "activity_cdc",
        "table.include.list": "public.salaries,public.referentiel_entreprise,public.activites_sportives",
        "plugin.name": "pgoutput",
        "snapshot.mode": "initial",
    },
}


def supprimer_connecteur_si_existant():
    """Supprime le connecteur existant pour forcer un nouveau snapshot complet."""
    reponse = requests.get(f"{KAFKA_CONNECT_URL}/connectors")
    reponse.raise_for_status()
    if CONNECTOR_CONFIG["name"] in reponse.json():
        requests.delete(f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_CONFIG['name']}")
        print(f"Ancien connecteur '{CONNECTOR_CONFIG['name']}' supprimé.")


def enregistrer_connecteur():
    """Enregistre le connecteur Debezium auprès de Kafka Connect."""
    reponse = requests.post(
        f"{KAFKA_CONNECT_URL}/connectors",
        json=CONNECTOR_CONFIG,
    )

    if reponse.status_code == 201:
        print(f"Connecteur '{CONNECTOR_CONFIG['name']}' créé avec succès.")
    else:
        print(f"Erreur lors de la création : {reponse.status_code}")
        print(reponse.text)


def verifier_statut():
    """Affiche l'état du connecteur et de sa tâche."""
    reponse = requests.get(
        f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_CONFIG['name']}/status"
    )
    reponse.raise_for_status()
    statut = reponse.json()
    print(f"État du connecteur : {statut['connector']['state']}")
    for tache in statut["tasks"]:
        print(f"État de la tâche {tache['id']} : {tache['state']}")


def main():
    supprimer_connecteur_si_existant()
    time.sleep(2)
    enregistrer_connecteur()
    time.sleep(3)
    verifier_statut()


if __name__ == "__main__":
    main()