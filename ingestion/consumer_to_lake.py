"""L1 — copier les déclarations brutes de Kafka ou d'un fichier JSONL dans DuckDB."""

import argparse
import hashlib
from contextlib import closing
from itertools import islice
from pathlib import Path
from time import monotonic

import duckdb
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

TOPIC = "maa.sinistres.declares.v1"
DATABASE = Path(__file__).resolve().parents[1] / "warehouse" / "maa.duckdb"
BATCH_SIZE = 1000


def prepare_bronze(db):
    db.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    db.execute("""
        CREATE TABLE IF NOT EXISTS bronze.sinistres_events (
            payload VARCHAR NOT NULL,
            partition INTEGER NOT NULL,
            "offset" BIGINT NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
            source VARCHAR NOT NULL,
            PRIMARY KEY (source, partition, "offset")
        )
    """)


def write_batch(db, source, partition, records):
    offsets, payloads = zip(*records)
    # Une instruction SQL écrit tout le lot ou rien, même en cas d'interruption.
    db.execute("""
        INSERT INTO bronze.sinistres_events (source, partition, "offset", payload)
        SELECT ?, ?, unnest(?::BIGINT[]), unnest(?::VARCHAR[])
        ON CONFLICT DO NOTHING
    """, [source, partition, offsets, payloads])


def load_file(db, path):
    with path.open("rb") as stream:
        # Chaque version du fichier possède ses propres positions, à partir de zéro.
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
        source = f"file:{path.name}:{digest}"
        stream.seek(0)
        rows = ((i, line.decode("utf-8")) for i, line in enumerate(stream))
        while batch := list(islice(rows, BATCH_SIZE)):
            write_batch(db, source, 0, batch)


def copy_partition(db, consumer, partition):
    source = f"kafka:{TOPIC}"
    tp = TopicPartition(TOPIC, partition)
    low, end = consumer.get_watermark_offsets(tp, timeout=10)
    saved = consumer.committed([tp], timeout=10)[0]
    if saved.error:
        raise KafkaException(saved.error)
    # Une base locale neuve doit pouvoir relire le canal même si Kafka a gardé sa position.
    db_next = db.execute("""
        SELECT max("offset") + 1 FROM bronze.sinistres_events
        WHERE source = ? AND partition = ?
    """, [source, partition]).fetchone()[0]
    start = max(low, min(saved.offset, db_next if db_next is not None else low))
    if start >= end:
        return

    consumer.assign([TopicPartition(TOPIC, partition, start)])
    position = start
    deadline = monotonic() + 20
    while position < end:
        records = []
        for msg in consumer.consume(BATCH_SIZE, timeout=0.5):
            error = msg.error()
            if error and error.code() != KafkaError._PARTITION_EOF:
                raise KafkaException(error)
            if msg.offset() >= end or error:
                position = msg.offset()
                break
            records.append((msg.offset(), msg.value().decode("utf-8")))
            position = msg.offset() + 1
        if records:
            write_batch(db, source, partition, records)
            deadline = monotonic() + 20
        elif monotonic() >= deadline and position < end:
            raise TimeoutError(f"Lecture Kafka interrompue sur la partition {partition}")

    # Confirmer seulement après l'écriture de toute la partition jusqu'à la borne lue.
    result = consumer.commit(offsets=[TopicPartition(TOPIC, partition, end)], asynchronous=False)
    for committed in result:
        if committed.error:
            raise KafkaException(committed.error)


def load_kafka(db):
    consumer = Consumer({
        "bootstrap.servers": "localhost:19092",
        "group.id": "maa-bronze",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "enable.partition.eof": True,
        "allow.auto.create.topics": False,
        "auto.offset.reset": "error",
    })
    with closing(consumer):
        topic = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC]
        if topic.error:
            raise KafkaException(topic.error)
        for partition in sorted(topic.partitions):
            copy_partition(db, consumer, partition)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-file", type=Path, help="Lire un fichier JSONL sans Kafka.")
    args = parser.parse_args()
    DATABASE.parent.mkdir(parents=True, exist_ok=True)
    with closing(duckdb.connect(str(DATABASE))) as db:
        prepare_bronze(db)
        if args.from_file:
            load_file(db, args.from_file)
        else:
            load_kafka(db)
        total = db.execute("SELECT count(*) FROM bronze.sinistres_events").fetchone()[0]
        print(f"bronze.sinistres_events : {total} lignes")


if __name__ == "__main__":
    main()
