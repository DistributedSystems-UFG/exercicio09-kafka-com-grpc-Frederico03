import json
import sqlite3
import threading
from concurrent import futures

import grpc
from google.protobuf import empty_pb2
from kafka import KafkaConsumer

from common import DB_PATH, GRPC_HOST, GRPC_PORT, KAFKA_BOOTSTRAP_SERVERS, PROCESSED_TOPIC
import temperature_service_pb2 as pb2
import temperature_service_pb2_grpc as pb2_grpc


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_id TEXT NOT NULL,
                source_timestamp TEXT NOT NULL,
                processed_at TEXT NOT NULL,
                temperature REAL NOT NULL,
                moving_average REAL NOT NULL
            )
            """
        )
        conn.commit()


def consume_processed_events() -> None:
    consumer = KafkaConsumer(
        PROCESSED_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="service-storage-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )

    print("Servico: consumidor Kafka iniciado para armazenamento...")

    for message in consumer:
        event = message.value
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                """
                INSERT INTO processed_events
                (sensor_id, source_timestamp, processed_at, temperature, moving_average)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    event["sensor_id"],
                    event["source_timestamp"],
                    event["processed_at"],
                    float(event["temperature"]),
                    float(event["moving_average_last_2h"]),
                ),
            )
            conn.commit()
        print("Evento processado salvo no banco.")


class TemperatureService(pb2_grpc.TemperatureServiceServicer):
    def _to_record(self, row: sqlite3.Row) -> pb2.TemperatureRecord:
        return pb2.TemperatureRecord(
            id=row[0],
            sensor_id=row[1],
            source_timestamp=row[2],
            processed_at=row[3],
            temperature=row[4],
            moving_average_last_2h=row[5],
        )

    def GetLatest(self, request: empty_pb2.Empty, context: grpc.ServicerContext) -> pb2.LatestResponse:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                """
                SELECT id, sensor_id, source_timestamp, processed_at, temperature, moving_average
                FROM processed_events
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()

        if row is None:
            return pb2.LatestResponse(found=False)

        return pb2.LatestResponse(found=True, record=self._to_record(row))

    def GetHistory(self, request: pb2.HistoryRequest, context: grpc.ServicerContext) -> pb2.HistoryResponse:
        limit = request.limit if request.limit > 0 else 10

        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute(
                """
                SELECT id, sensor_id, source_timestamp, processed_at, temperature, moving_average
                FROM processed_events
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        records = [self._to_record(row) for row in rows]
        return pb2.HistoryResponse(records=records)


def serve() -> None:
    init_db()

    consumer_thread = threading.Thread(target=consume_processed_events, daemon=True)
    consumer_thread.start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_TemperatureServiceServicer_to_server(TemperatureService(), server)

    address = f"{GRPC_HOST}:{GRPC_PORT}"
    server.add_insecure_port(address)
    server.start()

    print(f"gRPC server iniciado em {address}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
