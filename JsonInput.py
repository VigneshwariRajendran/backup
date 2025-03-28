#####Json input 
import sys
import json
import boto3
import uuid
import requests
from datetime import datetime
from io import StringIO
import pandas as pd
from awsglue.utils import getResolvedOptions

# Get arguments
args = getResolvedOptions(sys.argv, ['S3_BUCKET', 'S3_JSON_KEY', 'S3_JSON_KEY_2', 'S3_JSON_KEY_3', 'S3_OUTPUT_TRANSPOSED_KEY', 'MARQUEZ_URL'])

s3_bucket = args['S3_BUCKET']
json_file_key_1 = args['S3_JSON_KEY']  # First JSON file
json_file_key_2 = args['S3_JSON_KEY_2']  # Second JSON file
json_file_key_3 = args['S3_JSON_KEY_3']  # Third JSON file with different structure
output_csv_path = args['S3_OUTPUT_TRANSPOSED_KEY']
marquez_url = args['MARQUEZ_URL']
lineage_json_key = 'lineage-vr/lineage_events.json'

# Initialize S3 client
s3 = boto3.client('s3')

def read_json_from_s3(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj['Body'].read().decode('utf-8'))

def read_csv_from_s3(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

# Read datasets from multiple JSON files
datasets_1 = read_json_from_s3(s3_bucket, json_file_key_1)
datasets_2 = read_json_from_s3(s3_bucket, json_file_key_2)
datasets_3 = read_json_from_s3(s3_bucket, json_file_key_3)  # Different structure

# Normalize the third JSON structure
datasets_3_normalized = {key: {"columns": value} for key, value in datasets_3.items()}

# Merge all datasets
datasets = {**datasets_1, **datasets_2, **datasets_3_normalized}

df_output = read_csv_from_s3(s3_bucket, output_csv_path)

def generate_dataset_schema(namespace, dataset_name, columns, producer):
    return {
        "namespace": namespace,
        "name": dataset_name,
        "facets": {
            "schema": {
                "_producer": producer,
                "_schemaURL": f"http://example.com/schema_{dataset_name}",
                "fields": [{"name": col["name"], "type": col["type"]} for col in columns]
            }
        }
    }

def create_lineage_event(namespace, job_name, producer, inputs, outputs):
    return {
        "eventType": "COMPLETE",
        "eventTime": datetime.utcnow().isoformat() + 'Z',
        "producer": producer,
        "id": str(uuid.uuid4()),
        "job": {"namespace": namespace, "name": job_name},
        "run": {"runId": str(uuid.uuid4())},
        "inputs": inputs,
        "outputs": outputs
    }

namespace = "lineage-test"
lineage_events = {}
input_datasets = []

for rec_type, details in datasets.items():
    dataset_schema = generate_dataset_schema(namespace, rec_type, details["columns"], "json-source")
    input_datasets.append(dataset_schema)
    lineage_events[rec_type] = create_lineage_event(namespace, f"Extract-{rec_type}", "json-source", [], [dataset_schema])

# Define final output dataset
lineage_output = generate_dataset_schema(namespace, "output-extract", 
                                         [{"name": col, "type": str(df_output[col].dtype)} for col in df_output.columns],
                                         "transform-to-output")

# Create final lineage event
lineage_events["Final Output"] = create_lineage_event(namespace, "Transform-to-Output", "transform-to-output", 
                                                       input_datasets, [lineage_output])

def send_lineage_event(event, name):
    response = requests.post(f"{marquez_url}/api/v1/lineage", json=event)
    if response.status_code == 201:
        print(f"{name} lineage event successfully sent to Marquez.")
    else:
        print(f"Failed to send {name} lineage event. Status code: {response.status_code}, Response: {response.text}")

# Send lineage events
for name, event in lineage_events.items():
    send_lineage_event(event, name)

# Save lineage events as JSON in S3
s3.put_object(Bucket=s3_bucket, Key=lineage_json_key, Body=json.dumps(lineage_events))

#########################################JSON Key in arguments refers to input json files

