"""
generate_slack_messages.py
Consomme le topic Redpanda des activités sportives (alimenté par Debezium)
et poste un message de félicitations sur Slack pour chaque nouvelle activité.
Le snapshot initial (historique déjà en base au démarrage du connecteur) est
ignoré : seules les nouvelles insertions déclenchent un message Slack.
"""

import json
import requests
from confluent_kafka import Consumer
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.config import DATABASE_URL, SLACK_WEBHOOK_URL
from src.ingestion.models import Salarie

REDPANDA_BROKERS = "localhost:19092"
TOPIC = "activity_cdc.public.activites_sportives"


def recuperer_nom_salarie(session, id_salarie):
    """Récupère le prénom et nom d'un salarié depuis PostgreSQL."""
    salarie = session.get(Salarie, id_salarie)
    if salarie is None:
        return f"Salarié {id_salarie}"
    return f"{salarie.prenom} {salarie.nom}"


def formater_message(session, activite):
    """Construit le texte du message Slack pour une activité."""
    nom = recuperer_nom_salarie(session, activite["id_salarie"])
    type_activite = activite["type_activite"]

    date_debut = activite["date_debut"]
    date_fin = activite["date_fin"]
    duree_minutes = round((date_fin - date_debut) / 1_000_000 / 60)

    if activite.get("distance"):
        distance_km = activite["distance"]
        texte = (
            f"Bravo {nom} ! Tu viens de faire {distance_km} km de "
            f"{type_activite.lower()} en {duree_minutes} min !"
        )
    else:
        texte = (
            f"Bravo {nom} ! Séance de {type_activite.lower()} terminée "
            f"en {duree_minutes} min !"
        )

    if activite.get("commentaire"):
        texte += f" (\"{activite['commentaire']}\")"

    return texte


def envoyer_message_slack(texte):
    """Poste le message sur Slack via le webhook."""
    reponse = requests.post(SLACK_WEBHOOK_URL, json={"text": texte})
    reponse.raise_for_status()


def main():
    engine = create_engine(DATABASE_URL)

    consumer = Consumer({
        "bootstrap.servers": REDPANDA_BROKERS,
        "group.id": "slack-notifier-group",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    print(f"En écoute sur le topic '{TOPIC}'... (Ctrl+C pour arrêter)")

    try:
        with Session(engine) as session:
            while True:
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    print(f"Erreur de consommation : {message.error()}")
                    continue

                payload = json.loads(message.value())
                if payload.get("payload") is None:
                    continue  # message de contrôle Debezium, pas une vraie activité

                donnees = payload["payload"]
                if donnees["source"].get("snapshot") in ("true", "last"):
                    continue  # on ignore le snapshot initial

                activite = donnees["after"]
                if activite is None:
                    continue

                texte = formater_message(session, activite)
                envoyer_message_slack(texte)
                print(f"Message envoyé : {texte}")

    except KeyboardInterrupt:
        print("\nArrêt du consumer.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()