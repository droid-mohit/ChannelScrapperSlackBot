import json

import boto3
import requests

from env_vars import SLACK_URL, AWS_ACCESS_KEY, AWS_SECRET_KEY, METADATA_S3_BUCKET_NAME

s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)


def publish_message_to_slack(message_text):
    url = SLACK_URL
    payload = json.dumps({
        "text": message_text,
    })
    headers = {
        'Content-type': 'application/json'
    }
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        print(response.json())
    except Exception as e:
        print(f"Exception occurred while publishing message to slack with error: {e}")


def publish_json_blob_to_s3(key: str, bucket_name, json_blob: str):
    try:
        s3.put_object(Body=json_blob, Bucket=bucket_name, Key=key)
    except Exception as e:
        print(f"Exception occurred while publishing json blob: {json_blob} to s3 with error: {e}")


def publish_object_file_to_s3(file_path: str, bucket_name, object_key: str):
    try:
        s3.upload_file(file_path, bucket_name, object_key)
    except Exception as e:
        print(f"Exception occurred while publishing file: {file_path} to s3 with error: {e}")
