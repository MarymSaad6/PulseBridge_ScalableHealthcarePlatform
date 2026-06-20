from kafka import KafkaConsumer
import json
import boto3
import time
import uuid

# Kafka Consumer
consumer = KafkaConsumer(
    "chartevents-stream",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

# S3 client
s3 = boto3.client("s3")

bucket = "kafka-consumer-spark"
prefix = "consumer/chartevents/"

buffer = []
BATCH_SIZE = 100

print("Consumer started...")

for message in consumer:

    # add message to buffer
    buffer.append(message.value)

    # flush when batch size reached
    if len(buffer) >= BATCH_SIZE:

        filename = f"{prefix}batch_{int(time.time()*1000)}_{uuid.uuid4()}.json"

        try:
            s3.put_object(
                Bucket=bucket,
                Key=filename,
                Body=json.dumps(buffer)
            )

            print(f"Saved batch → s3://{bucket}/{filename}")

        except Exception as e:
            print("Error uploading to S3:", e)

        # reset buffer
        buffer = []

# flush remaining messages (important safety)
if buffer:

    filename = f"{prefix}batch_{int(time.time()*1000)}_{uuid.uuid4()}.json"

    s3.put_object(
        Bucket=bucket,
        Key=filename,
        Body=json.dumps(buffer)
    )

    print(f"Final flush → s3://{bucket}/{filename}")

consumer.close()
print("Consumer stopped.")