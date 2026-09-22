"""
generate_strava_data.py
Génère un historique simulé de 12 mois d'activités sportives par salarié,
basé sur les déclarations RH (Pratique d'un sport).
"""

import random
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.config import DATABASE_URL
from src.ingestion.models import Salarie, ActiviteSportive

import glob

TYPES_ACTIVITE = [
    "Course à pied", "Randonnée", "Vélo", "Natation",
    "Football", "Basketball", "Tennis", "Badminton",
    "Escalade", "Yoga", "Musculation",
]

# Distance non pertinente pour certains sports (ex: escalade, musculation)
SPORTS_SANS_DISTANCE = {"Escalade", "Musculation", "Yoga", "Football", "Basketball", "Tennis", "Badminton"}


def generer_activite(id_salarie, date_debut):
    """Génère une activité sportive réaliste pour un salarié à une date donnée."""
    type_activite = random.choice(TYPES_ACTIVITE)

    duree_minutes = random.randint(20, 120)
    date_fin = date_debut + timedelta(minutes=duree_minutes)

    if type_activite in SPORTS_SANS_DISTANCE:
        distance = None
    else:
        distance = round(random.uniform(2, 20), 1)  # km

    commentaire = None
    if random.random() < 0.15:  # 15% de chance d'avoir un commentaire
        commentaires_possibles = [
            "Belle sortie !", "Reprise du sport :)", "Nouveau record personnel",
            "Un peu difficile aujourd'hui", "Journée parfaite pour ça",
        ]
        commentaire = random.choice(commentaires_possibles)

    return ActiviteSportive(
        id_salarie=id_salarie,
        date_debut=date_debut,
        type_activite=type_activite,
        distance=distance,
        date_fin=date_fin,
        commentaire=commentaire,
    )


def determiner_frequence(pratique_sport):
    """Retourne le nombre d'activités sur 12 mois selon le profil du salarié."""
    if pd.notna(pratique_sport):
        return random.randint(15, 100)  # salarié sportif déclaré
    elif random.random() < 0.1:
        return random.randint(1, 10)  # salarié non déclaré, mais occasionnel
    else:
        return 0  # aucune activité


def main():
    engine = create_engine(DATABASE_URL)
    chemin_sport = glob.glob("data/raw/*Sportive*.xlsx")[0]
    df_sport = pd.read_excel(chemin_sport)

    with Session(engine) as session:
        salaries = session.query(Salarie).all()
        total_activites = 0

        for salarie in salaries:
            deja_historique = (
                session.query(ActiviteSportive)
                .filter_by(id_salarie=salarie.id_salarie)
                .first()
            )
            if deja_historique is not None:
                continue  # historique déjà généré pour ce salarié, ne pas dupliquer

            ligne_sport = df_sport[df_sport["ID salarié"] == salarie.id_salarie]
            pratique = ligne_sport["Pratique d'un sport"].values[0] if not ligne_sport.empty else None

            nb_activites = determiner_frequence(pratique)
            if nb_activites == 0:
                continue

            date_courante = datetime.now() - timedelta(days=365)
            for _ in range(nb_activites):
                if date_courante > datetime.now():
                    break  # ne jamais dépasser aujourd'hui (bug identifié plus tôt dans la conversation)

                activite = generer_activite(salarie.id_salarie, date_courante)
                session.add(activite)
                total_activites += 1

                date_courante += timedelta(days=random.randint(2, 15))

        session.commit()
        print(f"{total_activites} activité(s) générée(s) pour {len(salaries)} salarié(s) "
              f"(salariés ayant déjà un historique ignorés)")


if __name__ == "__main__":
    main()