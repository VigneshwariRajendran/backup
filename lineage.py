##### lineage script that worked ################
from pyspark.sql import SparkSession
from pyspark import conf
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark import SparkContext
from pyspark.sql.functions import expr
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col
import json
import sys
import boto3

# Initialize Glue context and Spark session
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Specify the input and output paths
input_path = "s3://ddsl-raw-developer/lineage-vr/input_transposed.csv"
output_path = "s3://ddsl-raw-developer/lineage-vr/output_transposed.csv"
lineage_output_path = "s3://ddsl-raw-developer/lineage-vr/lineage.json"

# Read the input and output CSV files into DataFrames with header and schema inference
input_df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
output_df = spark.read.option("header", "true").option("inferSchema", "true").csv(output_path)

# Print schemas and data for debugging
print("Input DataFrame Schema:")
input_df.printSchema()
print("Input DataFrame Sample Data:")
input_df.show(5)

print("Output DataFrame Schema:")
output_df.printSchema()
print("Output DataFrame Sample Data:")
output_df.show(5)

# Capture and compare column lineage between input and output DataFrames
input_columns = set(input_df.columns)
output_columns = set(output_df.columns)

# Log column comparisons
print("Input Columns: ", input_columns)
print("Output Columns: ", output_columns)

# Find common, input-only, and output-only columns
common_columns = input_columns.intersection(output_columns)
input_only_columns = input_columns.difference(output_columns)
output_only_columns = output_columns.difference(input_columns)

# Log the comparison results for lineage capture
print("Common Columns: ", common_columns)
print("Input-Only Columns: ", input_only_columns)
print("Output-Only Columns: ", output_only_columns)

# If there are columns to process, apply transformations accordingly
if common_columns:
    # Select only the common columns for lineage capture
    filtered_df = input_df.select([col(c) for c in common_columns])

    # Here you can apply any transformations if needed
    # For example, let's say you want to filter on a common column named 'value'
    # Check if 'value' exists in common_columns before filtering
    if 'value' in common_columns:
        filtered_df = filtered_df.filter(col('value') > 10)

    # Write the transformed data back to S3
    filtered_df.write.option("header", "true").csv(output_path, mode='overwrite')

    # Print the final transformed DataFrame schema
    print("Final Transformed DataFrame Schema:")
    filtered_df.printSchema()

else:
    print("No common columns to process between input and output.")
# Manually capture lineage (assuming your OpenLineage listener is running)
lineage_event = {
    "job": {
        "namespace": "vr-glue-jobs",
        "name": "vr_glue_job"
    },
    "inputs": [
        {
            "namespace": "s3://ddsl-raw-developer",
            "name": input_path,
            "facets": {
                "schema": {
                    "fields": [{"name": col, "type": str(input_df.schema[col].dataType)} for col in input_df.columns]
                }
            }
        }
    ],
    "outputs": [
        {
            "namespace": "s3://ddsl-raw-developer",
            "name": output_path,
            "facets": {
                "schema": {
                    "fields": [{"name": col, "type": str(output_df.schema[col].dataType)} for col in output_df.columns]
                }
            }
        }
    ]
}

# Convert the lineage event to JSON
lineage_json = json.dumps(lineage_event, indent=4)

# Save the JSON lineage to S3
s3 = boto3.client('s3')
bucket_name = lineage_output_path.split("/")[2]
key = "/".join(lineage_output_path.split("/")[3:])

s3.put_object(Bucket=bucket_name, Key=key, Body=lineage_json)


# Optionally, end the Spark session
spark.stop()
