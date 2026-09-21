from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey, DateTime, Numeric
from sqlalchemy.orm import declarative_base
from sqlalchemy import Boolean, Float
from sqlalchemy import DateTime
from datetime import datetime



Base = declarative_base()


class ReferentielEntreprise(Base):
    __tablename__ = "referentiel_entreprise"
    id_bu = Column(Integer, primary_key=True)
    nom_bu = Column(String, nullable=False)


class Salarie(Base):
    __tablename__ = "salaries"
    id_salarie = Column(Integer, primary_key=True)
    nom = Column(String, nullable=False)
    prenom = Column(String, nullable=False)
    date_naissance = Column(Date, nullable=False)
    id_bu = Column(Integer, ForeignKey("referentiel_entreprise.id_bu"), nullable=False)
    date_embauche = Column(Date, nullable=False)
    salaire_brut = Column(Integer, nullable=False)
    type_contrat = Column(String, nullable=False)
    nombre_jours_cp = Column(Integer, nullable=False)
    adresse_domicile = Column(String, nullable=False)
    moyen_deplacement = Column(String, nullable=False)


class DistanceDomicileTravail(Base):
    __tablename__ = "distances_domicile_travail"
    id_salarie = Column(Integer, ForeignKey("salaries.id_salarie"), primary_key=True)
    adresse_utilisee = Column(String, nullable=False)
    distance_km = Column(Float, nullable=False)


class ValidationDeplacement(Base):
    __tablename__ = "validations_deplacement"
    id_salarie = Column(Integer, ForeignKey("salaries.id_salarie"), primary_key=True)
    moyen_deplacement = Column(String, nullable=False)
    distance_km = Column(Float, nullable=False)
    seuil_km = Column(Integer, nullable=False)
    est_anomalie = Column(Boolean, nullable=False)


class ActiviteSportive(Base):
    __tablename__ = "activites_sportives"
    id = Column(Integer, primary_key=True, autoincrement=True)
    id_salarie = Column(Integer, ForeignKey("salaries.id_salarie"), nullable=False)
    date_debut = Column(DateTime, nullable=False)
    type_activite = Column(String, nullable=False)
    distance = Column(Float, nullable=True)  # vide si non pertinent (ex: escalade)
    date_fin = Column(DateTime, nullable=False)
    commentaire = Column(String, nullable=True)

class ReferentielSportif(Base):
    __tablename__ = "referentiel_sportif"
    id_sport = Column(Integer, primary_key=True, autoincrement=True)
    type_activite = Column(String, nullable=False, unique=True)
    categorie = Column(String, nullable=False)
    eligible_jours_bien_etre = Column(Boolean, nullable=False, default=True)

class ConfigAvantage(Base):
    __tablename__ = "config_avantages"
    id = Column(Integer, primary_key=True, autoincrement=True)
    cle = Column(String, nullable=False)
    valeur = Column(Numeric(10, 4), nullable=False)
    date_effet = Column(DateTime, nullable=False, default=datetime.now)