###9000,9004,9005 and 9019 #######
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
account_staging_9004 = 'lineage-vr/account_rectypes/9004_staging.csv'
account_staging_9005 = 'lineage-vr/account_rectypes/Account_9005/9005_staging.csv'
account_transpose_9005 = 'lineage-vr/account_rectypes/Account_9005/9005_transpose.csv'
account_staging_9019 = 'lineage-vr/account_rectypes/Account_9019/9019_staging.csv'
account_transpose_9019 = 'lineage-vr/account_rectypes/Account_9019/9019_transpose.csv'
lineage_json_key = 'lineage-vr/lineage_events.json'

# Initialize S3 client
s3 = boto3.client('s3')

# Function to read CSV data from S3
def read_csv_from_s3(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    csv_string = obj['Body'].read().decode('utf-8')
    return csv_string  # Returning the raw CSV string

# Read 9000.csv, 9005_staging.csv, and 9005_transpose.csv from S3
df_9000 = pd.read_csv(StringIO(read_csv_from_s3(s3_bucket, input_csv_path_9000)))
df_9004 = pd.read_csv(StringIO(read_csv_from_s3(s3_bucket, account_staging_9004)))
df_9005 = pd.read_csv(StringIO(read_csv_from_s3(s3_bucket, account_staging_9005)))
df_transpose_9005 = pd.read_csv(StringIO(read_csv_from_s3(s3_bucket, account_transpose_9005)))
df_9019 = pd.read_csv(StringIO(read_csv_from_s3(s3_bucket, account_staging_9019)))
df_transpose_9019 = pd.read_csv(StringIO(read_csv_from_s3(s3_bucket, account_transpose_9019)))
csv_string_output = read_csv_from_s3(s3_bucket, output_csv_path)
df_output = pd.read_csv(StringIO(csv_string_output), header=0)

# Extract the second row (for mapping 9000 and 9005 columns in output_transposed.csv)
second_row = pd.read_csv(StringIO(csv_string_output), skiprows=1, nrows=1, header=None)
second_row_values = second_row.iloc[0]  # Extract the first (and only) row

# Match which headers (columns) have values 9000 and 9005 in the second row
columns_matching_9000 = df_output.columns[second_row_values == 9000]
columns_matching_9004 = df_output.columns[second_row_values == 9004]
columns_matching_9005 = df_output.columns[second_row_values == 9005]
columns_matching_9019 = df_output.columns[second_row_values == 9019]  

# Create lineage DataFrame with the matching columns
df_lineage_9000 = df_output[list(columns_matching_9000)]
df_lineage_9004 = df_output[list(columns_matching_9004)]
df_lineage_9005 = df_output[list(columns_matching_9005)]
df_lineage_9019 = df_output[list(columns_matching_9019)]

# Define the new namespace and naming conventions
namespace = "rectype-poc"
event_time = datetime.utcnow().isoformat() + 'Z'

# Define input dataset schema (Account-Staging-9000 and Account-Staging-9005)
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

lineage_input_9004 = {
    "namespace": namespace,
    "name": "9004-Account-Staging",
    "facets": {
        "schema": {
            "_producer": "account-staging-process",
            "_schemaURL": "http://example.com/schema_9004",
            "fields": [{"name": col, "type": str(df_9004[col].dtype)} for col in df_9004.columns]
        }
    }
}

lineage_input_9005 = {
    "namespace": namespace,
    "name": "Account-Staging-9005",
    "facets": {
        "schema": {
            "_producer": "account-staging-process",
            "_schemaURL": "http://example.com/schema_9005",
            "fields": [{"name": col, "type": str(df_9005[col].dtype)} for col in df_9005.columns]
        }
    }
}
lineage_input_9019 = {
    "namespace": namespace,
    "name": "Account-Staging-9019",
    "facets": {
        "schema": {
            "_producer": "account-staging-process",
            "_schemaURL": "http://example.com/schema_9019",
            "fields": [{"name": col, "type": str(df_9019[col].dtype)} for col in df_9019.columns]
        }
    }
}
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
# Define intermediate dataset schema (intermediate-9004-extract and intermediate-9005-extract)
lineage_intermediate_9004 = {
    "namespace": namespace,
    "name": "intermediate-9004-extract",
    "facets": {
        "schema": {
            "_producer": "transform-input-to-intermediate",
            "_schemaURL": "http://example.com/schema_intermediate_9004",
            "fields": [{"name": col, "type": str(df_output[col].dtype)} for col in columns_matching_9004]
        }
    }
}

lineage_intermediate_9005 = {
    "namespace": namespace,
    "name": "intermediate-9005-extract",
    "facets": {
        "schema": {
            "_producer": "transform-input-to-intermediate",
            "_schemaURL": "http://example.com/schema_intermediate_9005",
            "fields": [{"name": col, "type": str(df_output[col].dtype)} for col in columns_matching_9005]
        }
    }
}

lineage_intermediate_9019 = {
    "namespace": namespace,
    "name": "intermediate-9019-extract",
    "facets": {
        "schema": {
            "_producer": "transform-input-to-intermediate",
            "_schemaURL": "http://example.com/schema_intermediate_9019",
            "fields": [{"name": col, "type": str(df_output[col].dtype)} for col in columns_matching_9019]
        }
    }
}


# Define final output dataset schema (output-extract)
lineage_output = {
    "namespace": namespace,
    "name": "output-extract",
    "facets": {
        "schema": {
            "_producer": "transform-intermediate-to-output",
            "_schemaURL": "http://example.com/schema_output",
            "fields": [{"name": col, "type": str(df_output[col].dtype)} for col in df_output.columns]
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

# Lineage event for 9004-Account-Staging
lineage_event_9004 = {
    "eventType": "COMPLETE",
    "eventTime": event_time,
    "producer": "account-staging-process",
    "id": str(uuid.uuid4()),
    "job": {
        "namespace": namespace,
        "name": "9004-Account-Staging-Process"
    },
    "run": {
        "runId": str(uuid.uuid4())
    },
    "inputs": [],
    "outputs": [lineage_input_9004]
}

# Define input dataset schema (Account-Staging-9005 and 9005_transpose.csv)
lineage_transpose_9005 = {
    "namespace": namespace,
    "name": "9005-transpose",
    "facets": {
        "schema": {
            "_producer": "transpose-staging-process",
            "_schemaURL": "http://example.com/schema_transpose_9005",
            "fields": [{"name": col, "type": str(df_transpose_9005[col].dtype)} for col in df_transpose_9005.columns]
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
            "fields": [{"name": col, "type": str(df_transpose_9019[col].dtype)} for col in df_transpose_9019.columns]
        }
    }
}

# Lineage event for Account-Staging-9005
lineage_event_9005 = {
    "eventType": "COMPLETE",
    "eventTime": event_time,
    "producer": "account-staging-process",
    "id": str(uuid.uuid4()),
    "job": {
        "namespace": namespace,
        "name": "Account-Staging-Process-9005"
    },
    "run": {
        "runId": str(uuid.uuid4())
    },
    "inputs": [],
    "outputs": [lineage_input_9005]
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
# Lineage event for transforming input 9004 to intermediate
lineage_event_intermediate_9004 = {
    "eventType": "COMPLETE",
    "eventTime": event_time,
    "producer": "transform-input-to-intermediate-9004",
    "id": str(uuid.uuid4()),
    "job": {
        "namespace": namespace,
        "name": "9004-mapping"
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
lineage_event_9019 = {
    "eventType": "COMPLETE",
    "eventTime": event_time,
    "producer": "account-staging-process",
    "id": str(uuid.uuid4()),
    "job": {
        "namespace": namespace,
        "name": "Account-Staging-Process-9019"
    },
    "run": {
        "runId": str(uuid.uuid4())
    },
    "inputs": [],
    "outputs": [lineage_input_9019]
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

# Lineage event for final output
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
        lineage_intermediate_9004, 
        lineage_intermediate_9005,
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
        print(f"Failed to send {name} lineage event. Status code: {response.status_code}, Response: {response.text}")

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

s3.put_object(Bucket=s3_bucket, Key=lineage_json_key, Body=json.dumps(lineage_events))
