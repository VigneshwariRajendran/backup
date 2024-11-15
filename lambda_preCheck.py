import json
import re
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta

# Initialize S3 client
s3_client = boto3.client('s3')

def load_config(bucket_name, config_key):
    
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=config_key)
        config_content = response['Body'].read().decode('utf-8')
        return json.loads(config_content)
    except ClientError as e:
        raise Exception(f"Error reading config file from S3: {e}")

def get_s3_files(bucket_name, prefix):
   
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' not in response:
            return []
        return [obj['Key'] for obj in response['Contents'] if not obj['Key'].endswith('/')]
    except ClientError as e:
        raise Exception(f"Error listing files in S3 bucket: {e}")

def validate_file_count(s3_files, expected_count):
   
    if len(s3_files) != expected_count:
        raise Exception(f"File count mismatch: Expected {expected_count}, Found {len(s3_files)}")

def validate_file_names(s3_files, prefix, patterns):
    # Get yesterday's date in 'yyyymmdd' format
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    
    for file_key in s3_files:
        file_name = file_key.replace(prefix, "")
        matched = False

        # Check if the file matches any of the specified patterns
        for pattern_name, pattern in patterns.items():
            match = re.fullmatch(pattern, file_name)
            
            if match:
                matched = True
                # Extract the date part using the capturing group
                date_part = match.group(1)
                
                # Check if the extracted date is valid and matches yesterday's date
                try:
                    file_date = datetime.strptime(date_part, '%Y%m%d')
                    if file_date.strftime('%Y%m%d') != yesterday:
                        raise Exception(f"File '{file_name}' does not match yesterday's date: {date_part}")
                except ValueError:
                    raise Exception(f"Invalid date format in file '{file_name}': {date_part}")
                break
        
        if not matched:
            raise Exception(f"File '{file_name}' does not match any naming convention.")


def lambda_handler(event, context):
   
    # Configuration location (update this as needed)
    config_bucket = 'ddsl-raw-developer'
    config_key = 'lambda-vr/config/precheck_lambda_config.json'
    
    # Step 1: Load the configuration
    config = load_config(config_bucket, config_key)
    file_count = config['file_count']
    s3_path = config['s3_path']
    naming_conventions = config['naming_conventions']
    
    # Parse bucket and prefix from s3_path
    bucket_name, prefix = s3_path.split('/', 1)
    
    # Step 2: Get list of files from the specified S3 path
    s3_files = get_s3_files(bucket_name, prefix)
    
    # Step 3: Validate file count
    validate_file_count(s3_files, file_count)
    
    # Step 4: Validate file names
    validate_file_names(s3_files, prefix, naming_conventions)
    
    # If all checks pass, return success
    return {
        'statusCode': 200,
        'body': json.dumps('Success: File count and naming conventions match!')
    }
