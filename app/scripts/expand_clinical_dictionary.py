from sqlalchemy.orm import Session
from uuid import uuid4
from datetime import datetime

from app.db.session import SessionLocal
from app.models.clinical_dictionary import ClinicalDictionary
from app.data.clinical_terms_extended import CLINICAL_TERMS


def normalize(text: str) -> str:
    return (
        text.lower()
        .strip()
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
    )


def expand_dictionary():
    db: Session = SessionLocal()

    inserted = 0

    try:
        for category, terms in CLINICAL_TERMS.items():

            print(f"Loading category: {category}")

            for term, icd_code in terms:

                normalized = normalize(term)

                exists = (
                    db.query(ClinicalDictionary)
                    .filter(ClinicalDictionary.term == term)
                    .first()
                )

                if exists:
                    continue

                new_entry = ClinicalDictionary(
                    id=uuid4(),
                    term=term,
                    icd10_code=icd_code,
                    priority=2,
                    created_at=datetime.utcnow(),
                )

                db.add(new_entry)
                inserted += 1

        db.commit()
        print(f"Inserted {inserted} new clinical terms")

    except Exception as e:
        db.rollback()
        print("Error:", e)

    finally:
        db.close()


if __name__ == "__main__":
    expand_dictionary()
