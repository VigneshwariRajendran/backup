###complete 9000 with datatype,atribute mapping with many-to-one mapping######
###with datatype of intermediate mapping also###########
import sys
import csv
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


# Function to process the updated structure of output_transposed.csv
def process_csv(csv_string):
    csv_reader = csv.DictReader(csv_string.splitlines())
    column_name_field ='Column Name'
    column_names = []
    datatype = []
    rec_type_values = []
    mappings = []
    join_conditions = []

    # Extract values from 'Column_name' and 'Rec_type'
    for row in csv_reader:
        column_names.append(row[column_name_field]) 
        datatype.append(row['Data Type'] if 'Data Type' in row else None)
        # Check if optional columns exist and add them to the lists
        rec_type_values.append(int(row['Rec_type']) if 'Rec_type' in row else None)
        mappings.append(row['Mapping'] if 'Mapping' in row else None)
        join_conditions.append(row['Join Condition'] if 'Join Condition' in row else None)

    return column_names, datatype, rec_type_values, mappings, join_conditions
# Updated logic for reading and processing the output CSV
csv_string_output = read_csv_from_s3(s3_bucket, output_csv_path)
column_names_output, datatype_output, rec_type_values, mappings, join_conditions = process_csv(csv_string_output)

# Read the input CSV and process it
csv_string_input = read_csv_from_s3(s3_bucket, input_csv_path_9000)
column_names_input,datatype_input, rec_type_values_input, mappings_input, join_conditions_input = process_csv(csv_string_input)

# Convert to DataFrame for input
df_input = pd.DataFrame({col: [] for col in column_names_input})

# Convert to DataFrame
df_output = pd.DataFrame({col: [] for col in column_names_output})  # Create DataFrame with the new column names

# Match which headers (columns) have values 9000 in 'Rec_type'
columns_matching_9000 = [col for col, rec_type in zip(column_names_output, rec_type_values) if rec_type == 9000]

# Identify columns with "Direct Mapping" and those that require composite key lineage
direct_mapping_columns = [col for col, mapping in zip(column_names_output, mappings) if mapping == 'Direct Mapping']
composite_key_columns = [col for col, mapping in zip(column_names_output, mappings) if mapping != 'Direct Mapping']

# Function to handle random datatype values (already in your script)
def handle_datatype(value):
    print(f"Processing datatype: {value}") 
    if isinstance(value, str) and len(value.strip()) > 0:
        normalized_value = value.strip().upper()
        return normalized_value
    else:
        return 'UNKNOWN (empty or not string)'

# Process the datatypes using the handle_datatype function
column_datatypes_input = [handle_datatype(val) for val in datatype_input]
column_datatypes_output = [handle_datatype(val) for val in datatype_output]

# Combine the column names and datatypes for the schema
schema_fields_input = [{"name": col, "type": column_datatypes_input[i]} for i, col in enumerate(column_names_input)]
schema_fields_output = [{"name": col, "type": column_datatypes_output[i]} for i, col in enumerate(column_names_output)]
# Define the intermediate dataset schema (using output column names and datatypes)
schema_fields_intermediate = [{"name": col, "type": column_datatypes_output[i]} for i, col in enumerate(columns_matching_9000)]

# Create lineage DataFrame with the matching columns
df_lineage_9000 = df_output[columns_matching_9000]

# Create lineage DataFrame with the matching columns
df_lineage_9000 = df_output[list(columns_matching_9000)]

# Define the new namespace and naming conventions
namespace = "restruct-datatype-0.1"
event_time = datetime.utcnow().isoformat() + 'Z'
# Match columns between 9000_staging and intermediate datasets
columns_9000_to_intermediate = [col for col in df_input.columns if col in df_lineage_9000.columns]

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
            "fields": schema_fields_input
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
            "fields": schema_fields_intermediate
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
            "fields": schema_fields_output
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
# Process composite key lineage
composite_key_lineage = {}
for col, join_condition in zip(composite_key_columns, join_conditions):
    if join_condition:  # Only process if a join condition exists
        input_fields = [
            {"namespace": namespace, "name": "intermediate-9000-extract", "field": field.strip()}
            for field in join_condition.split(',')
        ]
        composite_key_lineage[col] = {
            "inputFields": input_fields,
            "transformationDescription": f"Composite key derived from {', '.join(join_condition.split(','))}",
            "transformationType": "COMPOSITE_KEY"
        }
# Update lineage for composite key fields separately
def integrate_composite_key_lineage(fields, composite_key_lineage):
    # Add composite key lineage details explicitly
    for col, lineage_details in composite_key_lineage.items():
        fields[col] = lineage_details
    return fields

# Integrate composite key lineage into the 'columnLineage' fields
lineage_output["facets"]["columnLineage"]["fields"] = integrate_composite_key_lineage(
    lineage_output["facets"]["columnLineage"]["fields"],
    composite_key_lineage
)
# # Update the 'columnLineage' facet of the output dataset with composite key lineage
# lineage_output["facets"]["columnLineage"]["fields"].update(composite_key_lineage)

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
