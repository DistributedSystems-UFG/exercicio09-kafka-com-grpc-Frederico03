import json
from collections import deque
from datetime import datetime, timedelta, timezone

from kafka import KafkaConsumer, KafkaProducer

from common import KAFKA_BOOTSTRAP_SERVERS, PROCESSED_TOPIC, RAW_TOPIC

WINDOW_SECONDS = 7200


def parse_timestamp(value: str) -> datetime:
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value)


def main() -> None:
    consumer = KafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="processor-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    window = deque()

    print("Processor iniciado. Processando eventos do topico bruto...")

    try:
        for message in consumer:
            event = message.value
            ts = parse_timestamp(event["timestamp"])
            temp = float(event["temperature"])

            window.append((ts, temp))
            cutoff = ts - timedelta(seconds=WINDOW_SECONDS)
            while window and window[0][0] < cutoff:
                window.popleft()

            avg = sum(item[1] for item in window) / len(window)
            processed_event = {
                "sensor_id": event["sensor_id"],
                "source_timestamp": event["timestamp"],
                "processed_at": datetime.now(timezone.utc).isoformat(),
                "temperature": temp,
                "moving_average_last_2h": round(avg, 2),
            }

            producer.send(PROCESSED_TOPIC, processed_event)
            producer.flush()
            print(f"Evento processado em {PROCESSED_TOPIC}: {processed_event}")
    except KeyboardInterrupt:
        print("Encerrando processor...")
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
