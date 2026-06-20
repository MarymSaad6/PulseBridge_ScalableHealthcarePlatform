import boto3
import pandas as pd
import time
from kafka import KafkaProducer
import json
import io

# ---------------- Kafka Producer (Optimized) ----------------
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
    acks="all",
    linger_ms=10,
    batch_size=65536,
    compression_type="lz4"
)

# ---------------- S3 Client ----------------
s3 = boto3.client("s3")

bucket = "streaming-mimic"
prefix = "data-streams/outputevents-streams/"
# ---------------- Get Files ----------------
files = []

paginator = s3.get_paginator("list_objects_v2")

for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
    for obj in page.get("Contents", []):
        files.append(obj["Key"])

files.sort()

# ---------------- Streaming Loop ----------------
for file_key in files:

    print(f"\nProcessing: {file_key}")

    try:
        obj = s3.get_object(Bucket=bucket, Key=file_key)

        for chunk in pd.read_csv(
            io.BytesIO(obj["Body"].read()),
            header=0,
            chunksize=10000
        ):

            print(f"Chunk size: {len(chunk)}")

            records = chunk.to_dict("records")

            for row in records:

                event = {
                    "row_id": int(row["row_id"]) if pd.notna(row["row_id"]) else None,
                    "subject_id": int(row["subject_id"]) if pd.notna(row["subject_id"]) else None,
                    "hadm_id": int(row["hadm_id"]) if pd.notna(row["hadm_id"]) else None,
                    "icustay_id": int(row["icustay_id"]) if pd.notna(row["icustay_id"]) else None,
                    "charttime": str(row["charttime"]) if pd.notna(row["charttime"]) else None,
                    "itemid": int(row["itemid"]) if pd.notna(row["itemid"]) else None,
                    "value": float(row["value"]) if pd.notna(row["value"]) else None,
                    "valueuom": str(row["valueuom"]) if pd.notna(row["valueuom"]) else None,
                    "storetime": str(row["storetime"]) if pd.notna(row["storetime"]) else None,
                    "cgid": int(row["cgid"]) if pd.notna(row["cgid"]) else None,
                    "stopped": str(row["stopped"]) if pd.notna(row["stopped"]) else None,
                    "newbottle": int(row["newbottle"]) if pd.notna(row["newbottle"]) else None,
                    "iserror": int(row["iserror"]) if pd.notna(row["iserror"]) else None
                }

                key = f"{event['subject_id']}-{event['itemid']}".encode("utf-8")

                producer.send(
                    topic="outputstream-stream",
                    key=key,
                    value=event
                )

        # flush per file (safe balance between speed & safety)
        producer.flush()

        print(f"Finished: {file_key}")

    except Exception as e:
        print(f"Error processing {file_key}")
        print(e)

    print("Sleeping for 5 seconds before next file...")
    time.sleep(5)

producer.close()

print("All files processed.")