import io
import json
from typing import Dict, List
import boto3

s3_client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9020",
    aws_access_key_id="admin",
    aws_secret_access_key="password",
    region_name="us-east-1",
    config=boto3.session.Config(signature_version="s3v4"),  # Use s3v4 for MinIO
)

def load_events(bucket: str, keys: List[str]) -> Dict[str,bool]:
    data = {}
    for key in keys:
        byte_stream = io.BytesIO()
        s3_client.download_fileobj(
            Bucket=bucket,
            Key=key,
            Fileobj=byte_stream,
        )
        byte_stream.seek(0)

        for line in byte_stream:
            record = json.loads(line)
            event_id = record["event_id"]
            data[event_id] = True

    return data

input = load_events(bucket="input", keys=["mock_geo_data_100mb.json"])
print("input keys", len(input.keys()))

output = load_events(bucket="output", keys=["nospark/2026/01/01/part-0000.json","nospark/2026/01/02/part-0000.json","nospark/2026/01/03/part-0000.json"])
print("output keys", len(output.keys()))