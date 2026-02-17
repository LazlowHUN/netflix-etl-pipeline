FROM python:3.9-slim

RUN apt-get update && apt-get install -y gcc librdkafka-dev

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY avro_schemas/ ./avro_schemas/
COPY locustfile.py .

EXPOSE 8089

ENTRYPOINT ["locust", "-f", "locustfile.py"]