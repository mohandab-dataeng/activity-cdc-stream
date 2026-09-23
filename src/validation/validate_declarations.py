"""
validate_declarations.py
Étape 1 : calcule et stocke la distance domicile-travail de chaque salarié
          (géocodage via Nominatim, distance via un serveur OSRM local).
Étape 2 : applique les seuils selon le moyen de déplacement déclaré pour
          détecter les anomalies.
Règles (note de cadrage) :
  - Marche/running          -> max 15 km
  - Vélo/Trottinette/Autres -> max 25 km
"""

import os

import requests
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.config import DATABASE_URL
from src.ingestion.models import Salarie, DistanceDomicileTravail, ValidationDeplacement

ADRESSE_ENTREPRISE = os.getenv("ADRESSE_ENTREPRISE")
OSRM_URL = os.getenv("OSRM_URL", "http://localhost:5000")

SEUILS_KM = {
    "Marche/running": 15,
    "Vélo/Trottinette/Autres": 25,
}

# Client Nominatim — utilisé uniquement pour le géocodage (adresse -> coordonnées)
geolocalisateur = Nominatim(user_agent="activity-cdc-stream-projet12")
geocoder_avec_delai = RateLimiter(geolocalisateur.geocode, min_delay_seconds=1)


def geocoder_adresse(adresse):
    """Géocode une adresse via Nominatim (OpenStreetMap), gratuit et sans quota bas."""
    resultat = geocoder_avec_delai(adresse)
    if resultat is None:
        return None
    return [resultat.longitude, resultat.latitude]


def calculer_distance_km(coord_depart, coord_arrivee):
    """Distance routière via le serveur OSRM local (Docker), sans quota.

    Retourne None si OSRM est indisponible ou ne trouve aucun itinéraire
    (ex: adresse hors de la zone couverte) — un salarié problématique ne
    doit jamais interrompre le traitement des autres.
    """
    lon1, lat1 = coord_depart
    lon2, lat2 = coord_arrivee
    url = f"{OSRM_URL}/route/v1/driving/{lon1},{lat1};{lon2},{lat2}"
    try:
        reponse = requests.get(url, timeout=10)
        reponse.raise_for_status()
        data = reponse.json()
        distance_m = data["routes"][0]["distance"]
        return distance_m / 1000
    except (requests.exceptions.RequestException, KeyError, IndexError) as e:
        print(f"Erreur OSRM ({url}) : {e}")
        return None


def calculer_toutes_distances(session, coord_entreprise):
    """Calcule les distances via OSRM local (aucune limite, aucun sleep nécessaire)."""
    salaries = session.query(Salarie).all()
    a_calculer = []

    for salarie in salaries:
        distance_existante = session.get(DistanceDomicileTravail, salarie.id_salarie)
        if not (distance_existante and distance_existante.adresse_utilisee == salarie.adresse_domicile):
            a_calculer.append(salarie)

    if not a_calculer:
        print("Toutes les distances sont déjà à jour.")
        return

    print(f"{len(a_calculer)} salarié(s) à traiter...")

    for salarie in a_calculer:
        coord_domicile = geocoder_adresse(salarie.adresse_domicile)
        if coord_domicile is None:
            print(f"Adresse non géocodée : salarié {salarie.id_salarie}")
            continue

        distance_km = calculer_distance_km(coord_domicile, coord_entreprise)
        if distance_km is None:
            print(f"Distance non calculée (route introuvable) : salarié {salarie.id_salarie}")
            continue

        distance = DistanceDomicileTravail(
            id_salarie=salarie.id_salarie,
            adresse_utilisee=salarie.adresse_domicile,
            distance_km=round(distance_km, 1),
        )
        session.merge(distance)

    session.commit()
    print(f"{len(a_calculer)} distance(s) calculée(s)")


def valider_declarations(session):
    """Applique les seuils selon le mode déclaré, sans rappeler l'API."""
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