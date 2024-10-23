####9000 with datatype ###############
import sys
import boto3
import pandas as pd
from io import StringIO
import uuid
from datetime import datetime
import requests
import json
from awsglue.utils import getResolvedOptions

# Get arguments
args = getResolvedOptions(sys.argv, ['S3_BUCKET', 'S3_9000_KEY', 'S3_OUTPUT_TRANSPOSED_KEY', 'MARQUEZ_URL'])

s3_bucket = args['S3_BUCKET']
input_csv_path_9000 = args['S3_9000_KEY'] 
output_csv_path = args['S3_OUTPUT_TRANSPOSED_KEY'] 
marquez_url = args['MARQUEZ_URL'] 
account_staging_9005 = 'lineage-vr/Account_9005/9005_staging.csv'
account_transpose_9005 = 'lineage-vr/Account_9005/9005_transpose.csv'
lineage_json_key = 'lineage-vr/lineage_events.json'

# Initialize S3 client
s3 = boto3.client('s3')

# Function to read CSV data from S3
def read_csv_from_s3(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    csv_string = obj['Body'].read().decode('utf-8')
    return csv_string

# Read 9000.csv and output_transposed.csv
csv_string_9000 = read_csv_from_s3(s3_bucket, input_csv_path_9000)
csv_string_output = read_csv_from_s3(s3_bucket, output_csv_path)
df_output = pd.read_csv(StringIO(csv_string_output), header=0)
column_names_output = list(df_output.columns)
print(column_names_output)

# Extract the column names from the first row of the CSV
df_9000 = pd.read_csv(StringIO(csv_string_9000), header=0)
column_names_9000 = list(df_9000.columns)

# Extract the second row for datatype mapping
second_row_9000 = pd.read_csv(StringIO(csv_string_9000), skiprows=1, nrows=1, header=None)
second_row_values_9000 = second_row_9000.iloc[0]
# Extract the third row for datatype 
third_row = pd.read_csv(StringIO(csv_string_output), skiprows=2, nrows=1, header=None)
third_row_values = third_row.iloc[0]

# Function to handle random datatype values (already in your script)
def handle_datatype(value):
    print(f"Processing datatype: {value}") 
    if isinstance(value, str) and len(value.strip()) > 0:
        normalized_value = value.strip().upper()
        return normalized_value
    #     if 'VARCHAR' in normalized_value:
    #         return 'VARCHAR'
    #     elif 'CHAR' in normalized_value:
    #         return 'CHAR'
    #     elif 'TIMESTAMP' in normalized_value:
    #         return 'TIMESTAMP'
    #     elif 'DATE' in normalized_value:
    #         return 'DATE'
    #     elif 'INT' in normalized_value or 'INTEGER' in normalized_value:
    #         return 'INT'
    #     elif 'DECIMAL' in normalized_value or 'NUMERIC' in normalized_value:
    #         return 'int'
    #     elif 'FLOAT' in normalized_value or 'DOUBLE' in normalized_value:
    #         return 'FLOAT'
    #     else:
    #         return f'UNKNOWN ({normalized_value})'  # Improved UNKNOWN handling with original value
    else:
        return 'UNKNOWN (empty or not string)'
    # if isinstance(value, str) and len(value.strip()) > 0:
    #     if 'VARCHAR' in value.upper():
    #         return value.upper()
    #     elif 'CHAR' in value.upper():
    #         return value.upper()
    #     elif 'TIMESTAMP' in value.upper():
    #         return 'TIMESTAMP'
    #     elif 'DATE' in value.upper():
    #         return 'DATE'
    #     else:
    #         return 'UNKNOWN1'  # Default for any other types
    # else:
    #     return 'UNKNOWN2'

# Map the second-row values to datatypes (using handle_datatype function)
column_datatypes = [handle_datatype(val) for val in second_row_values_9000]
column_datatypes_output =  [handle_datatype(val) for val in third_row_values]
# for i, col in enumerate(column_names):
#     print(f"Column: {col}, Datatype: {column_datatypes[i]}")

# def extract_column_datatypes(row_values):
#     return [handle_datatype(val) for val in row_values]

# # Extract datatypes for both rows
# column_datatypes_9000 = extract_column_datatypes(second_row_values_9000)
# column_datatypes_output = extract_column_datatypes(third_row_values)
    

# Combine the column names and datatypes for the schema
schema_fields_9000 = [{"name": col, "type": column_datatypes[i]} for i, col in enumerate(column_names_9000)]
# schema_fields_output = [{"name": col, "type": val} for col, val in zip(column_names, third_row_values)]
schema_fields_output = [{"name": col, "type": column_datatypes_output[i]} for i, col in enumerate(column_names_output)]

# Create lineage JSON payload to send to Marquez
namespace = "datatype-poc-version-1.2"
event_time = datetime.utcnow().isoformat() + 'Z'
lineage_input_9000 = {
    "namespace": namespace,
    "name": "Account-Staging-9000",
    "facets": {
        "schema": {
            "_producer": "account-staging-process",
            "_schemaURL": "http://example.com/schema_9000",
            "fields": schema_fields_9000
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
# lineage_event = {
#     "eventType": "COMPLETE",
#     "eventTime": event_time,
#     "run": {
#         "runId": "your_run_id"
#     },
#     "job": {
#         "namespace": namespace,
#         "name": "Account-Staging-9000"
#     },
#     "inputs": [],
#     "outputs": [
#         {
#             "namespace": namespace,
#             "name": "Account-Staging-9000",
#             "facets": {
#                 "schema": {
#                     "_producer": "http://example.com/account-staging-process",
#                     "_schemaURL": "http://example.com/schema_9000",
#                     "fields": schema_fields
#                 }
#             }
#         }
#     ]
# }

# def send_lineage_event(event):
#     headers = {'Content-Type': 'application/json'}
#     response = requests.post(marquez_url + '/api/v1/lineage', headers=headers, data=json.dumps(event))
#     print(f"Lineage event sent, status code {response.status_code},  {response.text}")
#     return response.status_code 

# send_lineage_event(lineage_event_9000)
# print(json.dumps(lineage_event_9000, indent=4))


# # Initialize S3 client
# s3 = boto3.client('s3')

# # Function to read CSV data from S3
# def read_csv_from_s3(bucket, key):
#     obj = s3.get_object(Bucket=bucket, Key=key)
#     csv_string = obj['Body'].read().decode('utf-8')
#     return csv_string

# # Read 9000.csv, 9005_staging.csv, and 9005_transpose.csv from S3

# #df_9000 = pd.read_csv(StringIO(read_csv_from_s3(s3_bucket, input_csv_path_9000)))
# df_9005 = pd.read_csv(StringIO(read_csv_from_s3(s3_bucket, account_staging_9005)))
# df_transpose_9005 = pd.read_csv(StringIO(read_csv_from_s3(s3_bucket, account_transpose_9005)))
# csv_string_output = read_csv_from_s3(s3_bucket, output_csv_path)
# csv_string_9000 = read_csv_from_s3(s3_bucket, input_csv_path_9000)
# # df_9000 = pd.read_csv(StringIO(csv_string_9000), header=0)
# df_output = pd.read_csv(StringIO(csv_string_output), header=0)

# Extract the second row for datatype mapping
second_row = pd.read_csv(StringIO(csv_string_output), skiprows=1, nrows=1, header=None)
second_row_values = second_row.iloc[0]



# # df_9000 = pd.read_csv(StringIO(csv_string_9000))
# df_9000 = pd.read_csv(StringIO(csv_string_9000), header=0)

# second_row_9000 = pd.read_csv(StringIO(csv_string_9000), skiprows=1, nrows=1, header=None)
# second_row_values_9000= second_row_9000.iloc[0]
# print(second_row_values_9000)

# # A function to handle random datatype values and defaults
# def handle_datatype(value):
#     if isinstance(value, str) and len(value.strip()) > 0:
#         # Capture datatype values like varchar2(5), char(2), etc.
#         if 'VARCHAR' in value.upper():
#             return value.upper()
#         elif 'CHAR' in value.upper():
#             return value.upper()
#         elif 'TIMESTAMP' in value.upper():
#             return 'TIMESTAMP'
#         elif 'DATE' in value.upper():
#             return 'DATE'
#         else:
#             return 'UNKNOWN1'  # Default for any other types
#     else:
#         # Handle null/empty cells by assigning a default type
#         return 'UNKNOWN2'

# # Map second-row values to handle data types, including null cells
# column_datatypes = [handle_datatype(val) for val in second_row_values_9000]

# Match which headers (columns) have values 9000 and 9005 in the second row
columns_matching_9000 = df_output.columns[second_row_values == 9000]
# columns_matching_9005 = df_output.columns[second_row_values == 9005]

# # Create lineage DataFrame with the matching columns
# df_lineage_9000 = df_output[list(columns_matching_9000)]
# df_lineage_9005 = df_output[list(columns_matching_9005)]

# # Define the new namespace and naming conventions
# namespace = "datatype-poc-version1"
# event_time = datetime.utcnow().isoformat() + 'Z'

# # Define input dataset schema (Account-Staging-9000 and Account-Staging-9005)
# lineage_input_9000 = {
#     "namespace": namespace,
#     "name": "Account-Staging-9000",
#     "facets": {
#         "schema": {
#             "_producer": "account-staging-process",
#             "_schemaURL": "http://example.com/schema_9000",
#             "fields": [{"name": col, "type": column_datatypes[i]} for i, col in enumerate(df_9000.columns)]
#         }
#     }
# }

# # lineage_input_9005 = {
# #     "namespace": namespace,
# #     "name": "Account-Staging-9005",
# #     "facets": {
# #         "schema": {
# #             "_producer": "account-staging-process",
# #             "_schemaURL": "http://example.com/schema_9005",
# #             "fields": [{"name": col, "type": column_datatypes[i]} for i, col in enumerate(df_9005.columns)]
# #         }
# #     }
# # }

# Define intermediate dataset schema (intermediate-9000-extract and intermediate-9005-extract)
lineage_intermediate_9000 = {
    "namespace": namespace,
    "name": "intermediate-9000-extract",
    "facets": {
        "schema": {
            "_producer": "transform-input-to-intermediate",
            "_schemaURL": "http://example.com/schema_intermediate_9000",
           "fields": [{"name": col, "type": str(df_output[col].dtype)} for col in columns_matching_9000]
        }
    }
}

# # lineage_intermediate_9005 = {
# #     "namespace": namespace,
# #     "name": "intermediate-9005-extract",
# #     "facets": {
# #         "schema": {
# #             "_producer": "transform-input-to-intermediate",
# #             "_schemaURL": "http://example.com/schema_intermediate_9005",
# #             "fields": [{"name": col, "type": column_datatypes[i]} for i, col in enumerate(columns_matching_9005)]
# #         }
# #     }
# # }

# Define final output dataset schema (output-extract)
lineage_output = {
    "namespace": namespace,
    "name": "output-extract",
    "facets": {
        "schema": {
            "_producer": "transform-intermediate-to-output",
            "_schemaURL": "http://example.com/schema_output",
            "fields": schema_fields_output
        }
    }
}

# # Lineage event for Account-Staging-9000
# lineage_event_9000 = {
#     "eventType": "COMPLETE",
#     "eventTime": event_time,
#     "producer": "account-staging-process",
#     "id": str(uuid.uuid4()),
#     "job": {
#         "namespace": namespace,
#         "name": "Account-Staging-Process"
#     },
#     "run": {
#         "runId": str(uuid.uuid4())
#     },
#     "inputs": [],
#     "outputs": [lineage_input_9000]
# }

# # # Lineage event for Account-Staging-9005
# # lineage_event_9005 = {
# #     "eventType": "COMPLETE",
# #     "eventTime": event_time,
# #     "producer": "account-staging-process",
# #     "id": str(uuid.uuid4()),
# #     "job": {
# #         "namespace": namespace,
# #         "name": "Account-Staging-Process-9005"
# #     },
# #     "run": {
# #         "runId": str(uuid.uuid4())
# #     },
# #     "inputs": [],
# #     "outputs": [lineage_input_9005]
# # }

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

# # # Lineage event for transforming 9005 to transpose (9005-transpose.csv)
# # lineage_event_transpose_9005 = {
# #     "eventType": "COMPLETE",
# #     "eventTime": event_time,
# #     "producer": "transpose-staging-process",
# #     "id": str(uuid.uuid4()),
# #     "job": {
# #         "namespace": namespace,
# #         "name": "Transform-Staging-to-Transpose-9005"
# #     },
# #     "run": {
# #         "runId": str(uuid.uuid4())
# #     },
# #     "inputs": [lineage_input_9005],
# #     "outputs": [lineage_intermediate_9005]
# # }

# # # Lineage event for transforming transpose to intermediate (9005-transpose to Intermediate-9005)
# # lineage_event_intermediate_9005 = {
# #     "eventType": "COMPLETE",
# #     "eventTime": event_time,
# #     "producer": "transform-transpose-to-intermediate",
# #     "id": str(uuid.uuid4()),
# #     "job": {
# #         "namespace": namespace,
# #         "name": "Transform-Transpose-to-Intermediate-9005"
# #     },
# #     "run": {
# #         "runId": str(uuid.uuid4())
# #     },
# #     "inputs": [lineage_event_transpose_9005],
# #     "outputs": [lineage_intermediate_9005]
# # }

# Lineage event for transforming intermediate to output extract
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

# Send lineage events to Marquez
def send_lineage_event(event):
    headers = {'Content-Type': 'application/json'}
    response = requests.post(marquez_url + '/api/v1/lineage', headers=headers, data=json.dumps(event))
    print(f"Lineage event {event['id']} sent, status code {response.status_code}")
    return response.status_code

# send_lineage_event(lineage_event_9000)
# Send all events in sequence
for event in [lineage_event_9000, lineage_event_intermediate_9000, lineage_event_output]:
    send_lineage_event(event)

# # # Save lineage JSON to S3 for future reference
# # s3.put_object(Body=json.dumps([lineage_event_9000, lineage_event_9005, lineage_event_intermediate_9000, lineage_event_intermediate_9005, lineage_event_output]), Bucket=s3_bucket, Key=lineage_json_key)
