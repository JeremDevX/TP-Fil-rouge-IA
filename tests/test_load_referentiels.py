from contextlib import closing

import duckdb
import pytest

from ingestion.load_referentiels import COLUMNS, load_referentiels


def test_batch_replay_and_rollback(tmp_path):
    for table, columns in COLUMNS.items():
        (tmp_path / f"{table}.csv").write_text(",".join(columns) + "\n", encoding="utf-8")
    path = tmp_path / "contrats.csv"
    row = '0007,,auto,"champ, brut",,,,,\n'
    header = path.read_text()
    path.write_text(header + row * 2)

    with closing(duckdb.connect()) as db:
        load_referentiels(db, tmp_path)
        load_referentiels(db, tmp_path)
        assert db.execute("SELECT id_contrat, id_assure, formule FROM bronze.contrats").fetchall() == [
            ("0007", "", "champ, brut"), ("0007", "", "champ, brut"),
        ]

        path.write_text(header + row)
        last = tmp_path / "enquetes.csv"
        valid_header = last.read_text()
        last.write_text(valid_header + "valeur-confidentielle,champ-manquant\n")
        with pytest.raises(ValueError, match="enquetes.csv") as failure:
            load_referentiels(db, tmp_path)
        assert "valeur-confidentielle" not in str(failure.value)
        counts = "SELECT count(*), count(DISTINCT _source_sha256) FROM bronze.contrats"
        assert db.execute(counts).fetchone() == (2, 1)

        last.write_text(valid_header)
        load_referentiels(db, tmp_path)
        assert db.execute(counts).fetchone() == (3, 2)
