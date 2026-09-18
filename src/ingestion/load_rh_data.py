"""
load_rh_data.py
Charge les fichiers RH (salariés + pratique sportive) dans PostgreSQL.
"""

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.config import DATABASE_URL
from src.ingestion.models import Salarie, ReferentielEntreprise

import glob

CHEMIN_RH = glob.glob("data/raw/*RH*.xlsx")[0]
CHEMIN_SPORT = glob.glob("data/raw/*Sportive*.xlsx")[0]

def charger_referentiel_entreprise(session, df_rh):
    """Extrait les BU uniques et les insère dans referentiel_entreprise."""
    bu_uniques = sorted(df_rh["BU"].unique())  # tri alphabétique = ordre stable
    mapping_bu = {}

    for i, nom_bu in enumerate(bu_uniques, start=1):
        entree = ReferentielEntreprise(id_bu=i, nom_bu=nom_bu)
        session.merge(entree)
        mapping_bu[nom_bu] = i

    session.commit()
    print(f"{len(bu_uniques)} BU insérées dans referentiel_entreprise")
    return mapping_bu

def charger_salaries(session, df_rh, df_sport, mapping_bu):
    """Insère chaque salarié, avec sa BU (id) et son sport pratiqué (si déclaré)."""
    df_merged = df_rh.merge(
        df_sport[["ID salarié", "Pratique d'un sport"]],
        on="ID salarié",
        how="left"
    )

    for _, ligne in df_merged.iterrows():
        salarie = Salarie(
            id_salarie=ligne["ID salarié"],
            nom=ligne["Nom"],
            prenom=ligne["Prénom"],
            date_naissance=ligne["Date de naissance"],
            id_bu=mapping_bu[ligne["BU"]],
            date_embauche=ligne["Date d'embauche"],
            salaire_brut=ligne["Salaire brut"],
            type_contrat=ligne["Type de contrat"],
            nombre_jours_cp=ligne["Nombre de jours de CP"],
            adresse_domicile=ligne["Adresse du domicile"],
            moyen_deplacement=ligne["Moyen de déplacement"],
        )
        session.merge(salarie)

    session.commit()
    print(f"{len(df_merged)} salariés insérés dans salaries")

def main():
    engine = create_engine(DATABASE_URL)

    df_rh = pd.read_excel(CHEMIN_RH)
    df_sport = pd.read_excel(CHEMIN_SPORT)

    with Session(engine) as session:
        mapping_bu = charger_referentiel_entreprise(session, df_rh)
        charger_salaries(session, df_rh, df_sport, mapping_bu)

if __name__ == "__main__":
    main()