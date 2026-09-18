"""
validate_declarations.py
Étape 1 : calcule et stocke la distance domicile-travail de chaque salarié.
Étape 2 : applique les seuils selon le moyen de déplacement déclaré pour détecter les anomalies.
"""

import time
import openrouteservice
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.config import DATABASE_URL, OPENROUTESERVICE_API_KEY
from src.ingestion.models import Salarie, DistanceDomicileTravail, ValidationDeplacement

ADRESSE_ENTREPRISE = "1362 Avenue des Platanes, 34970 Lattes, France"

SEUILS_KM = {
    "Marche/running": 15,
    "Vélo/Trottinette/Autres": 25,
}

client = openrouteservice.Client(key=OPENROUTESERVICE_API_KEY)


def geocoder_adresse(adresse, max_tentatives=3):
    for tentative in range(max_tentatives):
        try:
            resultat = client.pelias_search(text=adresse)
            if not resultat["features"]:
                return None
            return resultat["features"][0]["geometry"]["coordinates"]
        except openrouteservice.exceptions.ApiError as e:
            if "Quota exceeded" in str(e) and tentative < max_tentatives - 1:
                print(f"Quota atteint, pause de 60s avant nouvelle tentative...")
                time.sleep(60)
            else:
                raise


def calculer_distance_km(coord_depart, coord_arrivee):
    matrice = client.distance_matrix(
        locations=[coord_depart, coord_arrivee],
        profile="driving-car",
        metrics=["distance"],
    )
    return matrice["distances"][0][1] / 1000


def calculer_toutes_distances(session, coord_entreprise):
    """Calcule les distances en un minimum d'appels API (1 seul appel matrix groupé)."""
    salaries = session.query(Salarie).all()
    a_calculer = []

    for salarie in salaries:
        distance_existante = session.get(DistanceDomicileTravail, salarie.id_salarie)
        if not (distance_existante and distance_existante.adresse_utilisee == salarie.adresse_domicile):
            a_calculer.append(salarie)

    if not a_calculer:
        print("Toutes les distances sont déjà à jour.")
        return

    print(f"{len(a_calculer)} salarié(s) à géocoder...")
    coords_domiciles = []
    for salarie in a_calculer:
        coord = geocoder_adresse(salarie.adresse_domicile)
        coords_domiciles.append(coord)
        time.sleep(2)

    toutes_locations = coords_domiciles + [coord_entreprise]
    index_entreprise = len(coords_domiciles)

    matrice = client.distance_matrix(
        locations=toutes_locations,
        sources=list(range(len(coords_domiciles))),
        destinations=[index_entreprise],
        profile="driving-car",
        metrics=["distance"],
    )

    for salarie, ligne_distance in zip(a_calculer, matrice["distances"]):
        distance_km = ligne_distance[0] / 1000
        distance = DistanceDomicileTravail(
            id_salarie=salarie.id_salarie,
            adresse_utilisee=salarie.adresse_domicile,
            distance_km=round(distance_km, 1),
        )
        session.merge(distance)

    session.commit()
    print(f"{len(a_calculer)} distance(s) calculée(s) en 1 seul appel matrix")

def valider_declarations(session):
    """Étape 2 : applique les seuils selon le mode déclaré, sans rappeler l'API."""
    salaries = session.query(Salarie).filter(
        Salarie.moyen_deplacement.in_(SEUILS_KM.keys())
    ).all()

    nb_anomalies = 0

    for salarie in salaries:
        distance_row = session.get(DistanceDomicileTravail, salarie.id_salarie)
        if distance_row is None:
            continue

        seuil = SEUILS_KM[salarie.moyen_deplacement]
        est_anomalie = distance_row.distance_km > seuil

        validation = ValidationDeplacement(
            id_salarie=salarie.id_salarie,
            moyen_deplacement=salarie.moyen_deplacement,
            distance_km=distance_row.distance_km,
            seuil_km=seuil,
            est_anomalie=est_anomalie,
        )
        session.merge(validation)

        if est_anomalie:
            nb_anomalies += 1
            print(f"ANOMALIE : salarié {salarie.id_salarie} déclare "
                  f"'{salarie.moyen_deplacement}' mais habite à "
                  f"{distance_row.distance_km} km (seuil : {seuil} km)")

    session.commit()
    print(f"\n{nb_anomalies} anomalie(s) détectée(s) sur {len(salaries)} salarié(s) vérifié(s)")


def main():
    engine = create_engine(DATABASE_URL)
    coord_entreprise = geocoder_adresse(ADRESSE_ENTREPRISE)

    if coord_entreprise is None:
        print("Impossible de géocoder l'adresse de l'entreprise. Arrêt.")
        return

    with Session(engine) as session:
        calculer_toutes_distances(session, coord_entreprise)
        valider_declarations(session)


if __name__ == "__main__":
    main()