"""
simulate_live_activities.py
Insère une nouvelle activité sportive toutes les 60 secondes, pour simuler
un flux "live" et tester la chaîne CDC -> Redpanda -> Spark -> Slack en démonstration.
Réutilise la logique de génération de generate_strava_data.py.
"""

import time
import random
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.config import DATABASE_URL
from src.ingestion.models import Salarie
from src.ingestion.generate_strava_data import generer_activite

INTERVALLE_SECONDES = 60


def main():
    engine = create_engine(DATABASE_URL)

    with Session(engine) as session:
        salaries = session.query(Salarie).all()

        print(f"Simulation live démarrée : une activité toutes les {INTERVALLE_SECONDES}s (Ctrl+C pour arrêter)")

        try:
            while True:
                salarie = random.choice(salaries)
                id_salarie, prenom, nom = salarie.id_salarie, salarie.prenom, salarie.nom

                activite = generer_activite(id_salarie, datetime.now())
                type_activite = activite.type_activite
                session.add(activite)
                session.commit()

                print(f"Activité insérée pour salarié {id_salarie} "
                      f"({prenom} {nom}) : {type_activite}")

                time.sleep(INTERVALLE_SECONDES)

        except KeyboardInterrupt:
            print("\nSimulation arrêtée.")


if __name__ == "__main__":
    main()