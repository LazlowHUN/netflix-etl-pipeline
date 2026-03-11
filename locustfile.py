import os
from locust import HttpUser, task, between
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

# Set Schema Registry connection
sr_config = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(sr_config)

# Read schema from file
with open("avro_schemas/play-event.avsc") as f:
    schema_str = f.read()

# Define Avro serializer
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

class NetflixTrafficGenerator(HttpUser):
    wait_time = between(1, 5) # Add a delay between two events

    def on_start(self):
        # Set Kafka connection
        self.producer = Producer({'bootstrap.servers': 'localhost:9092'})

    @task
    def send_play_event(self):
        # Generate random datas
        event_data = {
            "user_id": "user_" + str(os.urandom(2).hex()),
            "movie_id": "movie_123",
            "timestamp": 1676450000,
            "event_type": "START",
            "device_type": "SmartTV",
            "location": "Budapest"
        }

        # Send to kafka (avro format)
        self.producer.produce(
            topic='netflix_play_events',
            key=str(event_data['user_id']),
            value=avro_serializer(event_data, SerializationContext('netflix_play_events', MessageField.VALUE))
        )
        self.producer.flush()