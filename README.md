# Exercicio 09 - Kafka + gRPC (versao simples)

Implementacao minima do fluxo solicitado:

(1) produtor -> (2) consumidor/produtor -> (3) consumidor/web service <-> (4) cliente

## Arquitetura

1. `sensor_producer.py`
- Simula um sensor de temperatura e publica eventos no Kafka (`sensor.temperature.raw`).

2. `processor.py`
- Consome eventos brutos, calcula media movel da temperatura nas ultimas 2 horas e publica no Kafka (`sensor.temperature.processed`).

3. `service_server.py`
- Consome eventos processados, salva em SQLite (`temperature.db`) e expõe API gRPC para consultas.

4. `client.py`
- Cliente gRPC para consultar ultimo registro ou historico.

## Requisitos

- Docker / Docker Compose
- Python 3.10+

## Passo a passo

### 1) Subir Kafka (Redpanda)

Na raiz do projeto:

```bash
docker compose up -d
```

### 2) Instalar dependencias Python

```bash
pip install -r requirements.txt
```

### 3) Gerar stubs do gRPC

```bash
cd src
python -m grpc_tools.protoc -I./proto --python_out=. --grpc_python_out=. ./proto/temperature_service.proto
```

### 4) Executar os processos (em terminais separados)

Terminal A (servico gRPC + consumidor final):

```bash
cd src
python service_server.py
```

Terminal B (processador):

```bash
cd src
python processor.py
```

Terminal C (sensor/produtor):

```bash
cd src
python sensor_producer.py
```

### 5) Consultar com o cliente

Ultimo registro:

```bash
cd src
python client.py latest
```

Historico:

```bash
cd src
python client.py history --limit 10
```

## Observacoes

- A janela usada para media movel no processador e de 2 horas (7200 segundos), conforme enunciado.
- Para simplificar, o armazenamento usa SQLite local.
- Para encerrar o ambiente Kafka:

```bash
docker compose down
```
