"""
compute_kpis.py
Calcule les KPI finaux : prime sportive, jours bien-être, coût total.
Lit la couche silver (Delta Lake, via pandas) et les données PostgreSQL,
écrit les résultats dans une table dédiée pour la restitution Metabase.
"""

import glob
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, Float, Boolean
from sqlalchemy.orm import Session, declarative_base

from src.config import DATABASE_URL
from src.ingestion.models import Salarie, ValidationDeplacement

SILVER_PATH = "data/silver/activites_finales"
SEUIL_ACTIVITES_BIEN_ETRE = 15
TAUX_PRIME = 0.05
JOURS_BIEN_ETRE = 5

Base = declarative_base()


class KpiSalarie(Base):
    __tablename__ = "kpis_salaries"
    id_salarie = Column(Integer, primary_key=True)
    nb_activites_eligibles = Column(Integer, nullable=False)
    eligible_prime_sportive = Column(Boolean, nullable=False)
    eligible_jours_bien_etre = Column(Boolean, nullable=False)
    montant_prime = Column(Float, nullable=False)
    jours_bien_etre_accordes = Column(Integer, nullable=False)


def charger_activites_silver():
    """Lit tous les fichiers Parquet de la couche silver avec pandas."""
    fichiers_parquet = glob.glob(f"{SILVER_PATH}/*.parquet")
    df = pd.concat([pd.read_parquet(f) for f in fichiers_parquet], ignore_index=True)
    return df


def calculer_kpis(session, df_activites):
    """Calcule les KPI pour chaque salarié."""
    salaries = session.query(Salarie).all()
    resultats = []

    for salarie in salaries:
        activites_salarie = df_activites[df_activites["id_salarie"] == salarie.id_salarie]
        nb_eligibles = activites_salarie[
            activites_salarie["eligible_jours_bien_etre"] == True
        ].shape[0]

        eligible_bien_etre = nb_eligibles >= SEUIL_ACTIVITES_BIEN_ETRE

        validation = session.get(ValidationDeplacement, salarie.id_salarie)
        eligible_prime = (
            validation is not None
            and not validation.est_anomalie
        )

        montant_prime = salarie.salaire_brut * TAUX_PRIME if eligible_prime else 0.0
        jours_accordes = JOURS_BIEN_ETRE if eligible_bien_etre else 0

        resultats.append(KpiSalarie(
            id_salarie=salarie.id_salarie,
            nb_activites_eligibles=nb_eligibles,
            eligible_prime_sportive=eligible_prime,
            eligible_jours_bien_etre=eligible_bien_etre,
            montant_prime=montant_prime,
            jours_bien_etre_accordes=jours_accordes,
        ))

    return resultats


def main():
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)

    df_activites = charger_activites_silver()
    print(f"{len(df_activites)} activités chargées depuis la couche silver")

    with Session(engine) as session:
        resultats = calculer_kpis(session, df_activites)

        for resultat in resultats:
            session.merge(resultat)
        session.commit()

        cout_total_primes = sum(r.montant_prime for r in resultats)
        nb_eligibles_prime = sum(1 for r in resultats if r.eligible_prime_sportive)
        nb_eligibles_bien_etre = sum(1 for r in resultats if r.eligible_jours_bien_etre)

        print(f"\n{len(resultats)} salarié(s) traité(s)")
        print(f"{nb_eligibles_prime} salarié(s) éligible(s) à la prime sportive")
        print(f"{nb_eligibles_bien_etre} salarié(s) éligible(s) aux jours bien-être")
        print(f"Coût total des primes : {cout_total_primes:.2f} €")


if __name__ == "__main__":
    main()