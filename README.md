# Clinical Core

## Local (run migrations + seed)

```bash
alembic upgrade head
python -m app.scripts.load_icd10
python -m app.scripts.load_clinical_dictionary
uvicorn main:app --host 0.0.0.0 --port 8000
```

## Production (Railway)

Recommended start command:

```bash
alembic upgrade head && python -m app.scripts.startup_bootstrap && uvicorn main:app --host 0.0.0.0 --port $PORT
```

Notes:
- The ICD-10 loader is idempotent: it exits if the `icd10` table already has rows.
- Startup bootstrap enforces order: load ICD10 first, then rebuild/seed clinical dictionary.

## Verify data

```sql
SELECT COUNT(*) FROM icd10;
```
