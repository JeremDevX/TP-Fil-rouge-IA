from contextlib import closing
from unittest.mock import Mock

import duckdb
import pytest
from confluent_kafka import TopicPartition

from ingestion.consumer_to_lake import TOPIC, copy_partition, load_file, prepare_bronze


def test_file_replay_preserves_raw_duplicates(tmp_path):
    raw = '{ "test" : 1 }\n'
    path = tmp_path / "declarations.jsonl"
    path.write_text(raw * 2, encoding="utf-8")
    with closing(duckdb.connect()) as db:
        prepare_bronze(db)
        load_file(db, path)
        load_file(db, path)
        rows = db.execute("""
            SELECT payload, partition, "offset", ingested_at IS NOT NULL
            FROM bronze.sinistres_events ORDER BY "offset"
        """).fetchall()
        assert rows == [(raw, 0, 0, True), (raw, 0, 1, True)]


def test_retry_after_kafka_confirmation_fails():
    consumer = Mock()
    consumer.get_watermark_offsets.return_value = (8, 9)
    consumer.committed.return_value = [TopicPartition(TOPIC, 0, -1001)]
    consumer.consume.return_value = [
        Mock(**{"error.return_value": None, "offset.return_value": 8, "value.return_value": b"{}"})
    ]
    with closing(duckdb.connect()) as db:
        prepare_bronze(db)
        consumer.commit.side_effect = RuntimeError("confirmation indisponible")
        with pytest.raises(RuntimeError, match="confirmation indisponible"):
            copy_partition(db, consumer, 0)
        query = 'SELECT payload, "offset" FROM bronze.sinistres_events'
        assert db.execute(query).fetchall() == [("{}", 8)]
        consumer.commit.side_effect = None
        consumer.commit.return_value = [TopicPartition(TOPIC, 0, 9)]
        copy_partition(db, consumer, 0)
        assert db.execute(query).fetchall() == [("{}", 8)]
        assert consumer.commit.call_args.kwargs["offsets"][0].offset == 9
        assert consumer.commit.call_args.kwargs["asynchronous"] is False
