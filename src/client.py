import argparse

import grpc
from google.protobuf import empty_pb2

from common import GRPC_HOST, GRPC_PORT
import temperature_service_pb2 as pb2
import temperature_service_pb2_grpc as pb2_grpc


def print_record(record: pb2.TemperatureRecord) -> None:
    print(
        f"id={record.id} | sensor={record.sensor_id} | ts={record.source_timestamp} | "
        f"processed={record.processed_at} | temp={record.temperature} | "
        f"avg_2h={record.moving_average_last_2h}"
    )


def run_latest(stub: pb2_grpc.TemperatureServiceStub) -> None:
    response = stub.GetLatest(empty_pb2.Empty())
    if not response.found:
        print("Nenhum dado encontrado ainda.")
        return
    print_record(response.record)


def run_history(stub: pb2_grpc.TemperatureServiceStub, limit: int) -> None:
    response = stub.GetHistory(pb2.HistoryRequest(limit=limit))
    if not response.records:
        print("Nenhum dado encontrado ainda.")
        return

    for record in response.records:
        print_record(record)


def main() -> None:
    parser = argparse.ArgumentParser(description="Cliente gRPC para consultar temperaturas")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("latest", help="Consulta o ultimo registro")

    history_parser = subparsers.add_parser("history", help="Consulta historico")
    history_parser.add_argument("--limit", type=int, default=10)

    args = parser.parse_args()

    address = f"{GRPC_HOST}:{GRPC_PORT}"
    with grpc.insecure_channel(address) as channel:
        stub = pb2_grpc.TemperatureServiceStub(channel)

        if args.command == "latest":
            run_latest(stub)
        elif args.command == "history":
            run_history(stub, args.limit)


if __name__ == "__main__":
    main()
