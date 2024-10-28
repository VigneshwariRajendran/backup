####wrong column lineage is emitted but still a progress to see column lineage#####
import sys
import boto3
import pandas as pd
import requests
import uuid
import json
from io import StringIO
from datetime import datetime
from awsglue.utils import getResolvedOptions

# Get arguments
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
    return csv_string  # Returning the raw CSV string

# Read 9000.csv from S3
df_9000 = pd.read_csv(StringIO(read_csv_from_s3(s3_bucket, input_csv_path_9000)))
csv_string_output = read_csv_from_s3(s3_bucket, output_csv_path)
df_output = pd.read_csv(StringIO(csv_string_output), header=0)

# Extract the second row (for mapping 9000 columns in output_transposed.csv)
second_row = pd.read_csv(StringIO(csv_string_output), skiprows=1, nrows=1, header=None)
second_row_values = second_row.iloc[0]  # Extract the first (and only) row

# Match which headers (columns) have values 9000 in the second row
columns_matching_9000 = df_output.columns[second_row_values == 9000]

# Create lineage DataFrame with the matching columns
df_lineage_9000 = df_output[list(columns_matching_9000)]

# Define the new namespace and naming conventions
namespace = "account-staging-process-version-3.3"
event_time = datetime.utcnow().isoformat() + 'Z'

# Define input dataset schema (Account-Staging-9000)
lineage_input_9000 = {
    "namespace": namespace,
    "name": "Account-Staging-9000",
    "facets": {
        "schema": {
            "_producer": "account-staging-process",
            "_schemaURL": "http://example.com/schema_9000",
            "fields": [{"name": col, "type": str(df_9000[col].dtype)} for col in df_9000.columns]
        }
    }
}
# Lineage event for Account-Staging-9000
lineage_event_9000 = {
    "eventType": "COMPLETE",
    "eventTime": event_time,
    "producer": "account-staging-process",
    "id": str(uuid.uuid4()),
    "job": {
        "namespace": namespace,
        "name": "Account-Staging-Process"
    },
    "run": {
        "runId": str(uuid.uuid4())
    },
    "inputs": [],
    "outputs": [lineage_input_9000]
}


# Identify matching columns between intermediate and output datasets
matching_columns = [col for col in df_lineage_9000.columns if col in df_output.columns]

# Columns that exist in output but not in intermediate (new/derived columns)
derived_columns = [col for col in df_output.columns if col not in df_lineage_9000.columns]

# Define the intermediate dataset lineage (with column-level details)
lineage_intermediate_9000 = {
    "namespace": namespace,
    "name": "intermediate-9000-extract",
    "facets": {
        "schema": {
            "_producer": "transform-input-to-intermediate",
            "_schemaURL": "http://example.com/schema_intermediate_9000",
            "fields": [{"name": col, "type": str(df_output[col].dtype)} for col in matching_columns]
        }
    }
}

# Define the output dataset lineage (with column-level details)
lineage_output = {
    "namespace": namespace,
    "name": "output-extract",
    "facets": {
        "schema": {
            "_producer": "transform-intermediate-to-output",
            "_schemaURL": "http://example.com/schema_output",
            "fields": [{"name": col, "type": str(df_output[col].dtype)} for col in df_output.columns]
        },
        "columnLineage": {
            "_producer": "transform-intermediate-to-output",
            "_schemaURL": "http://example.com/schema_output",
            "fields": {
                # For each matching column, specify the input and output fields
                col: {
                    "inputFields": [
                        {
                            "namespace": namespace,
                            "name": "intermediate-9000-extract",
                            "field": col
                        }
                    ],
                    "transformationDescription": "Column was transferred without transformation",
                    "transformationType": "IDENTITY"
                } for col in matching_columns
            }
        }
    }
}

# Lineage event for transforming input 9000 to intermediate
lineage_event_intermediate_9000 = {
    "eventType": "COMPLETE",
    "eventTime": event_time,
    "producer": "transform-input-to-intermediate",
    "id": str(uuid.uuid4()),
    "job": {
        "namespace": namespace,
        "name": "Transform-Input-to-Intermediate"
    },
    "run": {
        "runId": str(uuid.uuid4())
    },
    "inputs": [lineage_input_9000],
    "outputs": [lineage_intermediate_9000]
}


# Lineage event for transforming intermediate to final output (column-level lineage)
lineage_event_output = {
    "eventType": "COMPLETE",
    "eventTime": event_time,
    "producer": "transform-intermediate-to-output",
    "id": str(uuid.uuid4()),
    "job": {
        "namespace": namespace,
        "name": "Transform-Intermediate-to-Output"
    },
    "run": {
        "runId": str(uuid.uuid4())
    },
    "inputs": [lineage_intermediate_9000],
    "outputs": [lineage_output]
}


# Send all lineage events to Marquez
def send_lineage_event(event, name):
    response = requests.post(f"{marquez_url}/api/v1/lineage", json=event)
    if response.status_code == 201:
        print(f"{name} lineage event successfully sent to Marquez.")
    else:
        print(f"Failed to send {name} lineage event. Status code: {response.status_code}, Response: {response.text}")

send_lineage_event(lineage_event_9000, "Account-Staging-9000")
send_lineage_event(lineage_event_intermediate_9000, "Intermediate-9000")
send_lineage_event(lineage_event_output, "Final Output")

# Optional: Save lineage events as a JSON file in S3
lineage_events = {
    "Account-Staging-9000": lineage_event_9000,
    "Intermediate-9000": lineage_event_intermediate_9000,
    "Final Output": lineage_event_output
}

s3.put_object(Bucket=s3_bucket, Key=lineage_json_key, Body=json.dumps(lineage_events))
##############################Almost correct column lineage#######################################################################################
import sys
import boto3
import pandas as pd
import requests
import uuid
import json
from io import StringIO
from datetime import datetime
from awsglue.utils import getResolvedOptions

# Get arguments
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
    return csv_string  # Returning the raw CSV string

# Read 9000.csv from S3
df_9000 = pd.read_csv(StringIO(read_csv_from_s3(s3_bucket, input_csv_path_9000)))
csv_string_output = read_csv_from_s3(s3_bucket, output_csv_path)
df_output = pd.read_csv(StringIO(csv_string_output), header=0)

# Extract the second row (for mapping 9000 columns in output_transposed.csv)
second_row = pd.read_csv(StringIO(csv_string_output), skiprows=1, nrows=1, header=None)
second_row_values = second_row.iloc[0]  # Extract the first (and only) row

# Match which headers (columns) have values 9000 in the second row
columns_matching_9000 = df_output.columns[second_row_values == 9000]

# Create lineage DataFrame with the matching columns
df_lineage_9000 = df_output[list(columns_matching_9000)]

# Define the new namespace and naming conventions
namespace = "column-lineage-version-1.0"
event_time = datetime.utcnow().isoformat() + 'Z'
# Match columns between 9000_staging and intermediate datasets
columns_9000_to_intermediate = [col for col in df_9000.columns if col in df_lineage_9000.columns]

# Match columns between intermediate and output_extract datasets
columns_intermediate_to_output = [col for col in df_lineage_9000.columns if col in df_output.columns]

# Define the input dataset schema for Account-Staging-9000
lineage_input_9000 = {
    "namespace": namespace,
    "name": "Account-Staging-9000",
    "facets": {
        "schema": {
            "_producer": "account-staging-process",
            "_schemaURL": "http://example.com/schema_9000",
            "fields": [{"name": col, "type": str(df_9000[col].dtype)} for col in df_9000.columns]
        }
    }
}

# Intermediate dataset schema (only matching columns between 9000_staging and intermediate)
lineage_intermediate_9000 = {
    "namespace": namespace,
    "name": "intermediate-9000-extract",
    "facets": {
        "schema": {
            "_producer": "transform-input-to-intermediate",
            "_schemaURL": "http://example.com/schema_intermediate_9000",
            "fields": [{"name": col, "type": str(df_lineage_9000[col].dtype)} for col in columns_9000_to_intermediate]
        },
        "columnLineage": {
            "_producer": "transform-input-to-intermediate",
            "_schemaURL": "http://example.com/schema_intermediate_9000",
            "fields": {
                col: {
                    "inputFields": [
                        {
                            "namespace": namespace,
                            "name": "Account-Staging-9000",
                            "field": col
                        }
                    ],
                    "transformationDescription": "Transferred without modification",
                    "transformationType": "IDENTITY"
                } for col in columns_9000_to_intermediate
            }
        }
    }
}

# Output dataset schema (only matching columns between intermediate and output)
lineage_output = {
    "namespace": namespace,
    "name": "output-extract",
    "facets": {
        "schema": {
            "_producer": "transform-intermediate-to-output",
            "_schemaURL": "http://example.com/schema_output",
            "fields": [{"name": col, "type": str(df_output[col].dtype)} for col in df_output.columns]
        },
        "columnLineage": {
            "_producer": "transform-intermediate-to-output",
            "_schemaURL": "http://example.com/schema_output",
            "fields": {
                col: {
                    "inputFields": [
                        {
                            "namespace": namespace,
                            "name": "intermediate-9000-extract",
                            "field": col
                        }
                    ],
                    "transformationDescription": "Transferred without modification",
                    "transformationType": "IDENTITY"
                } for col in columns_intermediate_to_output
            }
        }
    }
}

# Update and send lineage events
lineage_event_9000 = {
    "eventType": "COMPLETE",
    "eventTime": event_time,
    "producer": "account-staging-process",
    "id": str(uuid.uuid4()),
    "job": {
        "namespace": namespace,
        "name": "Account-Staging-Process"
    },
    "run": {
        "runId": str(uuid.uuid4())
    },
    "inputs": [],
    "outputs": [lineage_input_9000]
}

lineage_event_intermediate_9000 = {
    "eventType": "COMPLETE",
    "eventTime": event_time,
    "producer": "transform-input-to-intermediate",
    "id": str(uuid.uuid4()),
    "job": {
        "namespace": namespace,
        "name": "Transform-Input-to-Intermediate"
    },
    "run": {
        "runId": str(uuid.uuid4())
    },
    "inputs": [lineage_input_9000],
    "outputs": [lineage_intermediate_9000]
}

lineage_event_output = {
    "eventType": "COMPLETE",
    "eventTime": event_time,
    "producer": "transform-intermediate-to-output",
    "id": str(uuid.uuid4()),
    "job": {
        "namespace": namespace,
        "name": "Transform-Intermediate-to-Output"
    },
    "run": {
        "runId": str(uuid.uuid4())
    },
    "inputs": [lineage_intermediate_9000],
    "outputs": [lineage_output]
}

# Send all lineage events to Marquez
def send_lineage_event(event, name):
    response = requests.post(f"{marquez_url}/api/v1/lineage", json=event)
    if response.status_code == 201:
        print(f"{name} lineage event successfully sent to Marquez.")
    else:
        print(f"Failed to send {name} lineage event. Status code: {response.status_code}, Response: {response.text}")

send_lineage_event(lineage_event_9000, "Account-Staging-9000")
send_lineage_event(lineage_event_intermediate_9000, "Intermediate-9000")
send_lineage_event(lineage_event_output, "Final Output")

# Optional: Save lineage events as a JSON file in S3
lineage_events = {
    "Account-Staging-9000": lineage_event_9000,
    "Intermediate-9000": lineage_event_intermediate_9000,
    "Final Output": lineage_event_output
}

s3.put_object(Bucket=s3_bucket, Key=lineage_json_key, Body=json.dumps(lineage_events))
