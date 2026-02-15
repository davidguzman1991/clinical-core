import csv
from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from app.models.icd10 import ICD10


def load_icd10():
    db: Session = SessionLocal()

    # verificar si ya existe
    if db.query(ICD10).first():
        print("ICD10 already loaded")
        return

    with open("app/data/icd10_clean.csv", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            icd = ICD10(
                code=row["code"],
                description=row["description"]
            )
            db.add(icd)

    db.commit()
    print("ICD10 loaded successfully")


if __name__ == "__main__":
    load_icd10()
