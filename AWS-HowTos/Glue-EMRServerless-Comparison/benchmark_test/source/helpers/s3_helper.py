import boto3
from botocore.exceptions import ClientError


def upload_file_to_s3(local_file_path, bucket_name, s3_file_key):
    """
    Upload a file to an S3 bucket.

    :param local_file_path: File to upload.
    :param bucket_name: Bucket to upload to.
    :param s3_file_key: S3 object name. If not specified then file_name is used.
    :return: True if file was uploaded, else False.
    """
    # Create an S3 client
    s3_client = boto3.client("s3")

    try:
        s3_client.upload_file(local_file_path, bucket_name, s3_file_key)
        print(f"File {local_file_path} uploaded to {bucket_name}/{s3_file_key}")
        return True
    except ClientError as e:
        print(f"An error occurred: {e}")
        return False
