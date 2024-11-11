# 9000,9005, 9004 and 9019 complete working with output checking for composite key in all intermediate
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
args = getResolvedOptions(
    sys.argv, ['S3_BUCKET', 'S3_9000_KEY', 'S3_OUTPUT_TRANSPOSED_KEY', 'MARQUEZ_URL'])

s3_bucket = args['S3_BUCKET']
input_csv_path_9000 = args['S3_9000_KEY']
output_csv_path = args['S3_OUTPUT_TRANSPOSED_KEY']
marquez_url = args['MARQUEZ_URL']
account_staging_9004 = 'lineage-vr/account_rectypes/Account_9004_Staging.csv'
account_staging_9005 = 'lineage-vr/account_rectypes/Account_9005/Account_9005_Staging.csv'
account_transpose_9005 = 'lineage-vr/account_rectypes/Account_9005/9005_transpose.csv'
account_staging_9019 = 'lineage-vr/account_rectypes/Account_9019/Account_9019_Staging.csv'
account_transpose_9019 = 'lineage-vr/account_rectypes/Account_9019/9019_transpose.csv'
lineage_json_key = 'lineage-vr/lineage_events.json'
# Initialize S3 client
s3 = boto3.client('s3')

# Function to read CSV data from S3


def read_csv_from_s3(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    csv_string = obj['Body'].read().decode('utf-8')
    return csv_string  # Returning the raw CSV string

# Function to process the updated structure of output_transposed.csv


def process_csv(csv_string):
    csv_reader = csv.DictReader(csv_string.splitlines())
    column_name_field = 'Column Name'
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
        rec_type_values.append(
            int(row['Rec_type']) if 'Rec_type' in row else None)
        mappings.append(row['Mapping'] if 'Mapping' in row else None)
        join_conditions.append(row['Join Condition']
                               if 'Join Condition' in row else None)

    return column_names, datatype, rec_type_values, mappings, join_conditions


# Updated logic for reading and processing the output CSV
csv_string_output = read_csv_from_s3(s3_bucket, output_csv_path)
column_names_output, datatype_output, rec_type_values, mappings, join_conditions = process_csv(
    csv_string_output)

# Read the input CSV and process it
csv_string_input = read_csv_from_s3(s3_bucket, input_csv_path_9000)
column_names_input, datatype_input, rec_type_values_input, mappings_input, join_conditions_input = process_csv(
    csv_string_input)

# Read the 9004 CSV and process it
csv_string_9004 = read_csv_from_s3(s3_bucket, account_staging_9004)
column_names_9004, datatype_9004, rec_type_values_9004, mappings_9004, join_conditions_9004 = process_csv(
    csv_string_9004)

# Read the 9005 CSV and process it
csv_string_9005 = read_csv_from_s3(s3_bucket, account_staging_9005)
column_names_9005, datatype_9005, rec_type_values_9005, mappings_9005, join_conditions_9005 = process_csv(
    csv_string_9005)

# Read the 9005 transpose CSV and process it
csv_string_9005_transpose = read_csv_from_s3(s3_bucket, account_transpose_9005)
column_names_9005_transpose, datatype_9005_transpose, rec_type_values_9005_transpose, mappings_9005_transpose, join_conditions_9005_transpose = process_csv(
    csv_string_9005_transpose)

# Read the 9019 CSV and process it
csv_string_9019 = read_csv_from_s3(s3_bucket, account_staging_9019)
column_names_9019, datatype_9019, rec_type_values_9019, mappings_9019, join_conditions_9019 = process_csv(
    csv_string_9019)

# Read the 9019 CSV and process it
csv_string_9019_transpose = read_csv_from_s3(s3_bucket, account_transpose_9019)
column_names_9019_transpose, datatype_9019_transpose, rec_type_values_9019_transpose, mappings_9019_transpose, join_conditions_9019_transpose = process_csv(
    csv_string_9019_transpose)

# Convert to DataFrame for input
df_input = pd.DataFrame({col: [] for col in column_names_input})

# Convert to DataFrame
df_output = pd.DataFrame({col: [] for col in column_names_output})
df_9004 = pd.DataFrame({col: [] for col in column_names_9004})
df_9005 = pd.DataFrame({col: [] for col in column_names_9005})
df_9005_transpose = pd.DataFrame(
    {col: [] for col in column_names_9005_transpose})
df_9019 = pd.DataFrame({col: [] for col in column_names_9019})
df_9019_transpose = pd.DataFrame(
    {col: [] for col in column_names_9019_transpose})

# Match which headers (columns) have values 9000 in 'Rec_type'
columns_matching_9000 = [col for col, rec_type in zip(
    column_names_output, rec_type_values) if rec_type == 9000]
columns_matching_9005 = [col for col, rec_type in zip(
    column_names_output, rec_type_values) if rec_type == 9005]
columns_matching_9004 = [col for col, rec_type in zip(
    column_names_output, rec_type_values) if rec_type == 9004]
columns_matching_9019 = [col for col, rec_type in zip(
    column_names_output, rec_type_values) if rec_type == 9019]

# Identify columns with "Direct Mapping" and those that require composite key lineage
direct_mapping_columns = [col for col, mapping in zip(
    column_names_output, mappings) if mapping == 'Direct Mapping']
composite_key_columns = [col for col, mapping in zip(
    column_names_output, mappings) if mapping != 'Direct Mapping']

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
column_datatypes_9004 = [handle_datatype(val) for val in datatype_9004]
column_datatypes_9005 = [handle_datatype(val) for val in datatype_9005]
column_datatypes_9019 = [handle_datatype(val) for val in datatype_9019]
column_datatypes_9005_transpose = [
    handle_datatype(val) for val in datatype_9005_transpose]
column_datatypes_9019_transpose = [
    handle_datatype(val) for val in datatype_9019_transpose]

# Combine the column names and datatypes for the schema
schema_fields_input = [{"name": col, "type": column_datatypes_input[i]} for i, col in enumerate(column_names_input)]
schema_fields_output = [{"name": col, "type": column_datatypes_output[i]} for i, col in enumerate(column_names_output)]
schema_fields_9004 = [{"name": col, "type": column_datatypes_9004[i]} for i, col in enumerate(column_names_9004)]
schema_fields_9005 = [{"name": col, "type": column_datatypes_9005[i]} for i, col in enumerate(column_names_9005)]
schema_fields_9019 = [{"name": col, "type": column_datatypes_9019[i]} for i, col in enumerate(column_names_9019)]
schema_fields_9005_transpose = [{"name": col, "type": column_datatypes_9005_transpose[i]} for i, col in enumerate(column_names_9005_transpose)]
schema_fields_9019_transpose = [{"name": col, "type": column_datatypes_9019_transpose[i]} for i, col in enumerate(column_names_9019_transpose)]

# print("*******Account-9019*****")
# print(schema_fields_9019)
# print("******Transpose 9019*****")
# print(schema_fields_9019_transpose)
# Define the intermediate dataset schema (using output column names and datatypes)
schema_fields_intermediate = [{"name": col, "type": column_datatypes_output[i]} for i, col in enumerate(columns_matching_9000)]
schema_fields_intermediate_9005 = [{"name": col, "type": column_datatypes_output[i]} for i, col in enumerate(columns_matching_9005)]
schema_fields_intermediate_9004 = [{"name": col, "type": column_datatypes_output[i]} for i, col in enumerate(columns_matching_9004)]
schema_fields_intermediate_9019 = [{"name": col, "type": column_datatypes_output[i]} for i, col in enumerate(columns_matching_9019)]

# Create lineage DataFrame with the matching columns
df_lineage_9000 = df_output[list(columns_matching_9000)]
df_lineage_9005 = df_output[list(columns_matching_9005)]
df_lineage_9004 = df_output[list(columns_matching_9004)]
df_lineage_9019 = df_output[list(columns_matching_9019)]


# Define the new namespace and naming conventions
namespace = "complete-1.6"
event_time = datetime.utcnow().isoformat() + 'Z'

# To implement attribute level lineage
# Match columns between 9000_staging and intermediate datasets
columns_9000_to_intermediate = [col for col in df_input.columns if col in df_lineage_9000.columns]
columns_9004_to_intermediate = [col for col in df_9004.columns if col in df_lineage_9004.columns]
columns_9005_to_intermediate = [col for col in df_9005.columns if col in df_lineage_9005.columns]
columns_9019_to_intermediate = [col for col in df_9019.columns if col in df_lineage_9019.columns]

# Match columns between intermediate and output_extract datasets
columns_intermediate_to_output = [col for col in df_lineage_9000.columns if col in df_output.columns]

# Combine columns from both intermediate 9000 and intermediate 9005 to match with output dataset
columns_intermediate_to_output = list(set(columns_intermediate_to_output + columns_9005_to_intermediate + columns_9004_to_intermediate + columns_9019_to_intermediate))

# Define the input dataset schema for Account-Staging-9000
lineage_input_9000 = {
    "namespace": namespace,
    "name": "Account-9000-Staging",
    "facets": {
        "schema": {
            "_producer": "account-staging-process",
            "_schemaURL": "http://example.com/schema_9000",
            "fields": schema_fields_input
        }
    }
}

lineage_input_9004 = {
    "namespace": namespace,
    "name": "Account-9004-Staging",
    "facets": {
        "schema": {
            "_producer": "account-staging-process",
            "_schemaURL": "http://example.com/schema_9004",
            "fields": schema_fields_9004
        }
    }
}
lineage_input_9005 = {
    "namespace": namespace,
    "name": "Account-9005-Staging",
    "facets": {
        "schema": {
            "_producer": "account-staging-process",
            "_schemaURL": "http://example.com/schema_9005",
            "fields": schema_fields_9005
        }
    }
}
lineage_input_9019 = {
    "namespace": namespace,
    "name": "Account-9019-Staging",
    "facets": {
        "schema": {
            "_producer": "account-staging-process",
            "_schemaURL": "http://example.com/schema_9019",
            "fields": schema_fields_9019
        }
    }
}
# Define input dataset schema (Account-Staging-9005 and 9005_transpose.csv)
lineage_transpose_9005 = {
    "namespace": namespace,
    "name": "9005-transpose",
    "facets": {
        "schema": {
            "_producer": "transpose-staging-process",
            "_schemaURL": "http://example.com/schema_transpose_9005",
            "fields": schema_fields_9005_transpose
        },
        "columnLineage": {
            "_producer": "transform-input-to-intermediate",
            "_schemaURL": "http://example.com/schema_transpose_9005",
            "fields": {
                col: {
                    "inputFields": [
                        {
                            "namespace": namespace,
                            "name": "Account-9005-Staging",
                            "field": col
                        }
                    ],
                    "transformationDescription": "Transferred without modification",
                    "transformationType": "IDENTITY"
                } for col in columns_9005_to_intermediate
            }
        }
    }
}
lineage_transpose_9019 = {
    "namespace": namespace,
    "name": "9019-transpose",
    "facets": {
        "schema": {
            "_producer": "transpose-staging-process",
            "_schemaURL": "http://example.com/schema_transpose_9019",
            "fields": schema_fields_9019_transpose
        },
        "columnLineage": {
            "_producer": "transform-input-to-intermediate",
            "_schemaURL": "http://example.com/schema_intermediate_9019",
            "fields": {
                col: {
                    "inputFields": [
                        {
                            "namespace": namespace,
                            "name": "Account-9019-Staging",
                            "field": col
                        }
                    ],
                    "transformationDescription": "Transferred without modification",
                    "transformationType": "IDENTITY"
                } for col in columns_9019_to_intermediate
            }
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
                            "name": "Account-9000-Staging",
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
# Intermediate dataset schema (only matching columns between 9004_staging and intermediate)
lineage_intermediate_9004 = {
    "namespace": namespace,
    "name": "intermediate-9004-extract",
    "facets": {
        "schema": {
            "_producer": "transform-input-to-intermediate",
            "_schemaURL": "http://example.com/schema_intermediate_9000",
            "fields": schema_fields_intermediate_9004
        },
        "columnLineage": {
            "_producer": "transform-input-to-intermediate",
            "_schemaURL": "http://example.com/schema_intermediate_9000",
            "fields": {
                col: {
                    "inputFields": [
                        {
                            "namespace": namespace,
                            "name": "Account-9004-Staging",
                            "field": col
                        }
                    ],
                    "transformationDescription": "Transferred without modification",
                    "transformationType": "IDENTITY"
                } for col in columns_9004_to_intermediate
            }
        }
    }
}

# Intermediate dataset schema (only matching columns between 9005_staging and intermediate)
lineage_intermediate_9005 = {
    "namespace": namespace,
    "name": "intermediate-9005-extract",
    "facets": {
        "schema": {
            "_producer": "transform-input-to-intermediate",
            "_schemaURL": "http://example.com/schema_intermediate_9005",
            "fields": schema_fields_intermediate_9005
        },
        "columnLineage": {
            "_producer": "transform-input-to-intermediate",
            "_schemaURL": "http://example.com/schema_intermediate_9005",
            "fields": {
                col: {
                    "inputFields": [
                        {
                            "namespace": namespace,
                            "name": "9005-transpose",
                            "field": col
                        }
                    ],
                    "transformationDescription": "Transferred without modification",
                    "transformationType": "IDENTITY"
                } for col in columns_9005_to_intermediate
            }
        }
    }
}

# Intermediate dataset schema (only matching columns between 9005_staging and intermediate)
lineage_intermediate_9019 = {
    "namespace": namespace,
    "name": "intermediate-9019-extract",
    "facets": {
        "schema": {
            "_producer": "transform-input-to-intermediate",
            "_schemaURL": "http://example.com/schema_intermediate_9019",
            "fields": schema_fields_intermediate_9019
        },
        "columnLineage": {
            "_producer": "transform-input-to-intermediate",
            "_schemaURL": "http://example.com/schema_intermediate_9019",
            "fields": {
                col: {
                    "inputFields": [
                         # Check in 9019-transpose first
                        {
                            "namespace": namespace,
                            "name": "9019-transpose",
                            "field": col
                        }
                    ] if col in column_names_9019_transpose else [
                        # If not found in 9019-transpose, then check Account-9019-staging
                        {
                            "namespace": namespace,
                            "name": "Account-9019-Staging",
                            "field": col
                        }
                    ],
                    "transformationDescription": "Transferred without modification",
                    "transformationType": "IDENTITY"
                } for col in columns_9019_to_intermediate
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
                        },
                        {
                            "namespace": namespace,
                            "name": "intermediate-9005-extract",
                            "field": col
                        },
                        {
                            "namespace": namespace,
                            "name": "intermediate-9004-extract",
                            "field": col
                        },
                        {
                            "namespace": namespace,
                            "name": "intermediate-9019-extract",
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
# Define all the dataset names to check for composite key lineage
input_datasets = ["intermediate-9000-extract", "intermediate-9004-extract",
                  "intermediate-9005-extract", "intermediate-9019-extract"]

# Process composite key lineage for all datasets
composite_key_lineage = {}
for col, join_condition in zip(composite_key_columns, join_conditions):
    if join_condition:  # Only process if a join condition exists
        input_fields = []
        # Split the join condition and match fields from all datasets
        for field in join_condition.split(','):
            field = field.strip()
            for dataset in input_datasets:
                input_fields.append({
                    "namespace": namespace,
                    "name": dataset,
                    "field": field
                })

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
lineage_event_9004 = {
    "eventType": "COMPLETE",
    "eventTime": event_time,
    "producer": "account-9004-staging",
    "id": str(uuid.uuid4()),
    "job": {
        "namespace": namespace,
        "name": "Account-9004-Staging"
    },
    "run": {
        "runId": str(uuid.uuid4())
    },
    "inputs": [],
    "outputs": [lineage_input_9004]
}
# Lineage event for Account-Staging-9005
lineage_event_9005 = {
    "eventType": "COMPLETE",
    "eventTime": event_time,
    "producer": "account-9005-staging",
    "id": str(uuid.uuid4()),
    "job": {
        "namespace": namespace,
        "name": "Account-9005-Staging"
    },
    "run": {
        "runId": str(uuid.uuid4())
    },
    "inputs": [],
    "outputs": [lineage_input_9005]
}
lineage_event_9019 = {
    "eventType": "COMPLETE",
    "eventTime": event_time,
    "producer": "account-9019-staging",
    "id": str(uuid.uuid4()),
    "job": {
        "namespace": namespace,
        "name": "Account-9019-Staging"
    },
    "run": {
        "runId": str(uuid.uuid4())
    },
    "inputs": [],
    "outputs": [lineage_input_9019]
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
lineage_event_intermediate_9004 = {
    "eventType": "COMPLETE",
    "eventTime": event_time,
    "producer": "transform-input-to-intermediate",
    "id": str(uuid.uuid4()),
    "job": {
        "namespace": namespace,
        "name": "Transform-9004-to-Intermediate"
    },
    "run": {
        "runId": str(uuid.uuid4())
    },
    "inputs": [lineage_input_9004],
    "outputs": [lineage_intermediate_9004]
}


# Lineage event for transforming 9005 to transpose (9005-transpose.csv)
lineage_event_transpose_9005 = {
    "eventType": "COMPLETE",
    "eventTime": event_time,
    "producer": "transpose-staging-process",
    "id": str(uuid.uuid4()),
    "job": {
        "namespace": namespace,
        "name": "Transform-Staging-to-Transpose-9005"
    },
    "run": {
        "runId": str(uuid.uuid4())
    },
    "inputs": [lineage_input_9005],
    "outputs": [lineage_transpose_9005]
}
lineage_event_transpose_9019 = {
    "eventType": "COMPLETE",
    "eventTime": event_time,
    "producer": "transpose-staging-process",
    "id": str(uuid.uuid4()),
    "job": {
        "namespace": namespace,
        "name": "Transform-Staging-to-Transpose-9019"
    },
    "run": {
        "runId": str(uuid.uuid4())
    },
    "inputs": [lineage_input_9019],
    "outputs": [lineage_transpose_9019]
}

# Lineage event for transforming transpose to intermediate (9005-transpose to Intermediate-9005)
lineage_event_intermediate_9005 = {
    "eventType": "COMPLETE",
    "eventTime": event_time,
    "producer": "transform-transpose-to-intermediate",
    "id": str(uuid.uuid4()),
    "job": {
        "namespace": namespace,
        "name": "Transform-Transpose-to-Intermediate-9005"
    },
    "run": {
        "runId": str(uuid.uuid4())
    },
    "inputs": [lineage_transpose_9005],
    "outputs": [lineage_intermediate_9005]
}
lineage_event_intermediate_9019 = {
    "eventType": "COMPLETE",
    "eventTime": event_time,
    "producer": "transform-transpose-to-intermediate",
    "id": str(uuid.uuid4()),
    "job": {
        "namespace": namespace,
        "name": "Transform-Transpose-to-Intermediate-9019"
    },
    "run": {
        "runId": str(uuid.uuid4())
    },
    "inputs": [lineage_transpose_9019],
    "outputs": [lineage_intermediate_9019]
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
    "inputs": [
        lineage_intermediate_9000,
        lineage_intermediate_9005,
        lineage_intermediate_9004,
        lineage_intermediate_9019
    ],
    "outputs": [lineage_output]
}

# Send all lineage events to Marquez


def send_lineage_event(event, name):
    response = requests.post(f"{marquez_url}/api/v1/lineage", json=event)
    if response.status_code == 201:
        print(f"{name} lineage event successfully sent to Marquez.")
    else:
        print(
            f"Failed to send {name} lineage event. Status code: {response.status_code}, Response: {response.text}")


send_lineage_event(lineage_event_9000, "Account-Staging-9000")
send_lineage_event(lineage_event_9004, "9004-Account-Staging")
send_lineage_event(lineage_event_9005, "Account-Staging-9005")
send_lineage_event(lineage_event_transpose_9005, "9005-transpose")
send_lineage_event(lineage_event_intermediate_9000, "Intermediate-9000")
send_lineage_event(lineage_event_intermediate_9004, "Intermediate-9004")
send_lineage_event(lineage_event_intermediate_9005, "Intermediate-9005")
send_lineage_event(lineage_event_9019, "Account-Staging-9019")
send_lineage_event(lineage_event_transpose_9019, "9019-transpose")
send_lineage_event(lineage_event_intermediate_9019, "Intermediate-9019")
send_lineage_event(lineage_event_output, "Final Output")

# Optional: Save lineage events as a JSON file in S3
lineage_events = {
    "Account-Staging-9000": lineage_event_9000,
    "9004-Account-Staging": lineage_event_9004,
    "Account-Staging-9005": lineage_event_9005,
    "Intermediate-9000": lineage_event_intermediate_9000,
    "Intermediate-9004": lineage_event_intermediate_9004,
    "Intermediate-9005": lineage_event_intermediate_9005,
    "Account-Staging-9019": lineage_event_9019,
    "9019-transpose": lineage_event_transpose_9019,
    "Intermediate-9019": lineage_event_intermediate_9019,
    "Final Output": lineage_event_output
}

s3.put_object(Bucket=s3_bucket, Key=lineage_json_key,
              Body=json.dumps(lineage_events))
