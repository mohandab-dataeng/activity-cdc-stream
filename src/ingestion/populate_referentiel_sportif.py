"""
populate_referentiel_sportif.py
Insère les catégories de sport dans le référentiel sportif.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.config import DATABASE_URL
from src.ingestion.models import ReferentielSportif

SPORTS = [
    ("Course à pied", "Cardio", True),
    ("Vélo", "Cardio", True),
    ("Natation", "Cardio", True),
    ("Randonnée", "Cardio", True),
    ("Football", "Sport co", True),
    ("Basketball", "Sport co", True),
    ("Tennis", "Sport co", True),
    ("Badminton", "Sport co", True),
    ("Escalade", "Force", True),
    ("Musculation", "Force", True),
    ("Yoga", "Bien-être", True),
]


def main():
    engine = create_engine(DATABASE_URL)
    with Session(engine) as session:
        for type_activite, categorie, eligible in SPORTS:
            sport = ReferentielSportif(
                type_activite=type_activite,
                categorie=categorie,
                eligible_jours_bien_etre=eligible,
            )
            session.merge(sport)
        session.commit()
        print(f"{len(SPORTS)} types de sport insérés dans referentiel_sportif")


if __name__ == "__main__":
    main()