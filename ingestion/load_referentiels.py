"""L1 — charger les quatre exports CSV bruts dans DuckDB, en un seul lot."""

import csv
import hashlib
import io
from contextlib import closing
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
COLUMNS = {
    "assures": "id_assure civilite prenom nom sexe date_naissance email telephone iban code_postal commune zone csp date_adhesion".split(),
    "contrats": "id_contrat id_assure produit formule date_debut date_fin prime_annuelle franchise plafond_garantie".split(),
    "prestataires": "id_prestataire raison_sociale type_prestataire code_postal agree_maa siret".split(),
    "enquetes": "id_sinistre date_ouverture date_cloture conclusion enqueteur".split(),
}


def load_export(db, directory, table, columns):
    path = directory / f"{table}.csv"
    content = path.read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    fields = ", ".join(f"{column} VARCHAR" for column in columns)
    db.execute(f"CREATE TABLE IF NOT EXISTS bronze.{table} ({fields}, _source_sha256 VARCHAR NOT NULL)")
    if db.execute(f"SELECT 1 FROM bronze.{table} WHERE _source_sha256 = ? LIMIT 1", [digest]).fetchone():
        return

    # Lire les mêmes octets que ceux de l'empreinte, même si un export est remplacé.
    with io.TextIOWrapper(io.BytesIO(content), encoding="utf-8-sig", newline="") as stream:
        rows = csv.reader(stream, strict=True)
        if next(rows, None) != columns:
            raise ValueError(f"{path.name} : en-tête inattendu")
        batch = []
        for row in rows:
            if len(row) != len(columns):
                raise ValueError(f"{path.name} : nombre de champs incorrect à la ligne {rows.line_num}")
            batch.append((*row, digest))
    if batch:
        names = ", ".join([*columns, "_source_sha256"])
        placeholders = ", ".join("?" for _ in range(len(columns) + 1))
        db.executemany(f"INSERT INTO bronze.{table} ({names}) VALUES ({placeholders})", batch)


def load_referentiels(db, directory):
    db.execute("BEGIN TRANSACTION")
    try:
        db.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        for table, columns in COLUMNS.items():
            try:
                load_export(db, directory, table, columns)
            except (duckdb.Error, csv.Error, UnicodeError) as error:
                # Le lecteur CSV peut inclure une ligne confidentielle dans son erreur.
                raise ValueError(f"{table}.csv : import impossible ({type(error).__name__})") from None
        db.execute("COMMIT")
    except Exception:
        db.execute("ROLLBACK")
        raise


if __name__ == "__main__":
    database = ROOT / "warehouse" / "maa.duckdb"
    database.parent.mkdir(parents=True, exist_ok=True)
    with closing(duckdb.connect(str(database))) as db:
        load_referentiels(db, ROOT / "data" / "raw")
        for table in COLUMNS:
            count = db.execute(f"SELECT count(*) FROM bronze.{table}").fetchone()[0]
            print(f"bronze.{table} : {count} lignes")
