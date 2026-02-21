from sqlalchemy import insert
from app.db.session import SessionLocal
from app.models.clinical_ontology import ClinicalOntology

TERMS = [
    # Gastrointestinal
    {"term": "gastrointestinal", "system": "digestivo"},
    {"term": "digestivo", "system": "digestivo"},
    {"term": "estomago", "system": "digestivo", "organ": "estomago"},
    {"term": "gastrico", "system": "digestivo", "organ": "estomago"},
    {"term": "esofago", "system": "digestivo", "organ": "esofago"},
    {"term": "intestinal", "system": "digestivo"},
    {"term": "colon", "system": "digestivo", "organ": "colon"},
    {"term": "rectal", "system": "digestivo", "organ": "recto"},
    {"term": "hepatica", "system": "digestivo", "organ": "higado"},
    {"term": "biliar", "system": "digestivo"},
    {"term": "pancreatico", "system": "digestivo", "organ": "pancreas"},

    # Endocrino
    {"term": "endocrino", "system": "endocrino"},
    {"term": "tiroides", "system": "endocrino", "organ": "tiroides"},
    {"term": "tiroideo", "system": "endocrino", "organ": "tiroides"},
    {"term": "paratiroides", "system": "endocrino"},
    {"term": "hipofisis", "system": "endocrino"},
    {"term": "pituitaria", "system": "endocrino"},
    {"term": "suprarrenal", "system": "endocrino"},
    {"term": "adrenal", "system": "endocrino"},
    {"term": "metabolico", "system": "endocrino"},

    # Cardiovascular
    {"term": "cardiovascular", "system": "cardiovascular"},
    {"term": "cardiaco", "system": "cardiovascular"},
    {"term": "miocardico", "system": "cardiovascular"},
    {"term": "vascular", "system": "cardiovascular"},
    {"term": "arterial", "system": "cardiovascular"},
    {"term": "venoso", "system": "cardiovascular"},

    # Respiratorio
    {"term": "respiratorio", "system": "respiratorio"},
    {"term": "pulmonar", "system": "respiratorio"},
    {"term": "bronquial", "system": "respiratorio"},
    {"term": "pleural", "system": "respiratorio"},

    # Neurologico
    {"term": "neurologico", "system": "neurologico"},
    {"term": "cerebral", "system": "neurologico"},
    {"term": "encefalico", "system": "neurologico"},
    {"term": "medular", "system": "neurologico"},
    {"term": "periferico", "system": "neurologico"},
]

def normalize(text: str) -> str:
    return text.lower().strip()

def run():
    db = SessionLocal()
    try:
        for item in TERMS:
            db.add(
                ClinicalOntology(
                    term=item["term"],
                    normalized_term=normalize(item["term"]),
                    system=item.get("system"),
                    organ=item.get("organ"),
                    weight=0.10,
                )
            )
        db.commit()
        print("Phase 2A ontology seeded successfully.")
    finally:
        db.close()

if __name__ == "__main__":
    run()