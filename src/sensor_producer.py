import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from common import KAFKA_BOOTSTRAP_SERVERS, RAW_TOPIC


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    sensor_id = "sensor-1"
    current_temp = 25.0

    print("Sensor producer iniciado. Publicando eventos de variacao significativa...")

    try:
        while True:
            delta = random.uniform(-1.2, 1.2)
            next_temp = current_temp + delta

            if abs(delta) >= 0.5:
                event = {
                    "sensor_id": sensor_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "temperature": round(next_temp, 2),
                }
                producer.send(RAW_TOPIC, event)
                producer.flush()
                print(f"Evento publicado em {RAW_TOPIC}: {event}")

            current_temp = next_temp
            time.sleep(2)
    except KeyboardInterrupt:
        print("Encerrando sensor producer...")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
