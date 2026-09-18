from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey
from sqlalchemy.orm import declarative_base
from sqlalchemy import Boolean, Float


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