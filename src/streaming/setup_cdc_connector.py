"""
setup_cdc_connector.py
Enregistre le connecteur Debezium PostgreSQL auprès de Kafka Connect,
pour capturer les changements sur salaries et referentiel_entreprise
et les publier dans Redpanda.
"""

import requests

KAFKA_CONNECT_URL = "http://localhost:8083"

CONNECTOR_CONFIG = {
    "name": "postgres-activites-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "admin",
        "database.password": "admin",
        "database.dbname": "sportdata",
        "topic.prefix": "activity_cdc",
        "table.include.list": "public.salaries,public.referentiel_entreprise",
        "plugin.name": "pgoutput",
    },
}


def connecteur_existe_deja():
    """Vérifie si le connecteur est déjà enregistré."""
    reponse = requests.get(f"{KAFKA_CONNECT_URL}/connectors")
    reponse.raise_for_status()
    return CONNECTOR_CONFIG["name"] in reponse.json()


def enregistrer_connecteur():
    """Enregistre le connecteur Debezium auprès de Kafka Connect."""
    if connecteur_existe_deja():
        print(f"Le connecteur '{CONNECTOR_CONFIG['name']}' existe déjà, aucune action.")
        return

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
    enregistrer_connecteur()
    verifier_statut()


if __name__ == "__main__":
    main()