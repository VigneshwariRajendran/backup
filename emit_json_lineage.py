################## lineage that sends json as output 08-10-2024 ##################################
import boto3
import pandas as pd
import requests
from io import StringIO
from awsglue.utils import getResolvedOptions
import sys
from datetime import datetime
import uuid
import json

# Glue job parameters
args = getResolvedOptions(sys.argv, ['S3_BUCKET', 'S3_9000_KEY', 'S3_OUTPUT_TRANSPOSED_KEY', 'MARQUEZ_URL'])

s3_bucket = args['S3_BUCKET']
input_csv_path_9000 = args['S3_9000_KEY'] 
output_csv_path = args['S3_OUTPUT_TRANSPOSED_KEY'] 
marquez_url = args['MARQUEZ_URL'] 
lineage_json_key = 'lineage-vr/lineage_events.json'

# Initialize S3 client
s3 = boto3.client('s3')

# Function to read CSV data from S3
def read_csv_from_s3(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    csv_string = obj['Body'].read().decode('utf-8')
    return pd.read_csv(StringIO(csv_string))

# Read 9000.csv and output_transposed.csv from S3
df_9000 = read_csv_from_s3(s3_bucket, input_csv_path_9000)
df_output = read_csv_from_s3(s3_bucket, output_csv_path)

# Define the namespace for Marquez lineage
namespace = "rectype-lineage"

# Get the current timestamp in ISO 8601 format
event_time = datetime.utcnow().isoformat() + 'Z'

# Create field schema for 9000.csv (input)
input_fields = [{"name": col, "type": str(df_9000[col].dtype)} for col in df_9000.columns]

# Define lineage for input_transposed.csv (input dataset)
lineage_input_9000 = {
    "namespace": "Account-Staging-9000",
    "name": "Account-Staging-9000",
    "facets": {
        "schema": {
            "_producer": "rectype-lineage",
            "_schemaURL": "http://example.com/schema_9000",
            "fields": input_fields
        }
    }
}

# Create an input job for input_transposed.csv
input_job = {
    "namespace": "rectype-lineage",
    "name": "Account-Staging-9000"
}

# Create field schema for output_transposed.csv (output)
output_fields = [{"name": col, "type": str(df_output[col].dtype)} for col in df_output.columns]

# Define lineage for output_transposed.csv (output dataset)
lineage_output = {
    "namespace": "rectype-lineage",
    "name": "output-extract",
    "facets": {
        "schema": {
            "_producer": "rectype-lineage",
            "_schemaURL": "http://example.com/schema_output",
            "fields": output_fields
        }
    }
}

# Create the lineage event for input_transposed.csv
lineage_event_input = {
    "eventType": "COMPLETE",
    "eventTime": event_time,
    "producer": "rectype-lineage",
    "id": str(uuid.uuid4()),
    "job": input_job,
    "run": {
        "runId": str(uuid.uuid4())
    },
    "inputs": [],
    "outputs": [lineage_input_9000]
}

# Send the input lineage event to Marquez using the REST API
response_input = requests.post(f"{marquez_url}/api/v1/lineage", json=lineage_event_input)

# Check if the response is successful
if response_input.status_code == 201:
    print("Input lineage event successfully sent to Marquez.")
else:
    print(f"Failed to send input lineage event. Status code: {response_input.status_code}, Response: {response_input.text}")

# Create the lineage event for the output_transposed.csv
lineage_event_output = {
    "eventType": "COMPLETE",
    "eventTime": event_time,
    "producer": "rectype-lineage",
    "id": str(uuid.uuid4()),
    "job": {
        "namespace": "rectype-lineage",
        "name": "output-extract"
    },
    "run": {
        "runId": str(uuid.uuid4())
    },
    "inputs": [lineage_input_9000],  # Link input job to output dataset
    "outputs": [lineage_output]
}

# Combine both lineage events into one JSON object
combined_lineage_events = {
    "input_lineage": lineage_event_input,
    "output_lineage": lineage_event_output
}

# Convert the combined lineage events to JSON format
lineage_json = json.dumps(combined_lineage_events, indent=4)

# Save the lineage JSON file to S3
s3.put_object(Bucket=s3_bucket, Key=lineage_json_key, Body=lineage_json)


# Send the output lineage event to Marquez using the REST API
response_output = requests.post(f"{marquez_url}/api/v1/lineage", json=lineage_event_output)

# Check if the response is successful
if response_output.status_code == 201:
    print("Output lineage event successfully sent to Marquez.")
else:
    print(f"Failed to send output lineage event. Status code: {response_output.status_code}, Response: {response_output.text}")
