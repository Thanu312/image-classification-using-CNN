import os
import json
import boto3
from botocore.exceptions import ClientError

# Initialize the S3 and DynamoDB clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['DYNAMODB_TABLE_NAME'])

def read_file_from_s3(bucket_name, file_key):
    """Read a JSON file from an S3 bucket."""
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)  # Assuming the file is in JSON format
    except ClientError as e:
        print(f"Error reading file from S3: {e}")
    except json.JSONDecodeError:
        print("Error decoding JSON from the file.")
    return None

def upload_to_dynamodb(item):
    """Upload an item to DynamoDB."""
    try:
        table.put_item(Item=item)
        print(f"Added item to DynamoDB table {os.environ['DYNAMODB_TABLE_NAME']}.")
    except ClientError as e:
        print(f"Error adding item to DynamoDB: {e}")

def lambda_handler(event, context):
    # Get the S3 bucket name and file key from environment variables
    bucket_name = os.environ['S3_BUCKET_NAME']
    file_key = os.environ['FILE_KEY']  # e.g., 'data/myfile.json'

    # Read content from the S3 bucket
    file_content = read_file_from_s3(bucket_name, file_key)
    if file_content:
        # Upload each item to DynamoDB
        if isinstance(file_content, list):
            for item in file_content:  # Assuming the file contains a list of items
                upload_to_dynamodb(item)
        else:
            # If not a list, upload directly
            upload_to_dynamodb(file_content)

    return {
        'statusCode': 200,
        'body': json.dumps('Process completed.')
    }
