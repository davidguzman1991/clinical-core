from sqlalchemy import Column, Integer, String, Float
from app.db.base import Base


class ClinicalOntology(Base):
    __tablename__ = "clinical_ontology"

    id = Column(Integer, primary_key=True, index=True)
    term = Column(String(120), nullable=False)
    normalized_term = Column(String(120), nullable=False, index=True)
    system = Column(String(80), nullable=True)
    organ = Column(String(80), nullable=True)
    functional_group = Column(String(80), nullable=True)
    related_prefix = Column(String(5), nullable=True)
    weight = Column(Float, nullable=False, default=0.10)