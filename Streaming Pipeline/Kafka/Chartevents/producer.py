import boto3
import pandas as pd
import time
from kafka import KafkaProducer
import json
import io

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
    acks="all",
    linger_ms=10,          
    batch_size=65536       
)

s3 = boto3.client("s3")

bucket = "streaming-mimic"
prefix = "data-streams/chartevents-streams/"

files = []

paginator = s3.get_paginator("list_objects_v2")

for page in paginator.paginate(
    Bucket=bucket,
    Prefix=prefix
):
    for obj in page.get("Contents", []):
        files.append(obj["Key"])

files.sort()

for file_key in files:

    print(f"\nProcessing: {file_key}")

    try:

        obj = s3.get_object(
            Bucket=bucket,
            Key=file_key
        )

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
                    "itemid": int(row["itemid"]) if pd.notna(row["itemid"]) else None,
                    "charttime": str(row["charttime"]) if pd.notna(row["charttime"]) else None,
                    "storetime": str(row["storetime"]) if pd.notna(row["storetime"]) else None,
                    "cgid": int(row["cgid"]) if pd.notna(row["cgid"]) else None,
                    "value": str(row["value"]) if pd.notna(row["value"]) else None,
                    "valuenum": float(row["valuenum"]) if pd.notna(row["valuenum"]) else None,
                    "valueuom": str(row["valueuom"]) if pd.notna(row["valueuom"]) else None,
                    "warning": int(row["warning"]) if pd.notna(row["warning"]) else None,
                    "error": int(row["error"]) if pd.notna(row["error"]) else None,
                    "resultstatus": str(row["resultstatus"]) if pd.notna(row["resultstatus"]) else None,
                    "stopped": str(row["stopped"]) if pd.notna(row["stopped"]) else None
                }

                producer.send(
                    topic="chartevents-stream",
                    key=(
                        str(event["subject_id"]).encode("utf-8")
                        if event["subject_id"] is not None
                        else b"unknown"
                    ),
                    value=event
                )

        producer.flush()

        print(f"Finished: {file_key}")

    except Exception as e:
        print(f"Error processing {file_key}")
        print(e)

    print("Waiting 5 seconds before next file...")
    time.sleep(5)

producer.close()

print("All files processed.")