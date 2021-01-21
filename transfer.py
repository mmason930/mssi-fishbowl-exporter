import logging
import boto3
import os
import argparse
from botocore.exceptions import ClientError


def upload_file(file_name, bucket, object_name=None):

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


## Main. ##
if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--sourcedir', help='the source directory', required=True)
    parser.add_argument('--accesskey', help='the AWS access key', required=True)
    parser.add_argument('--secretkey', help='the AWS secret key', required=True)
    parser.add_argument('--bucket', help='the Amazon S3 bucket name', required=True)
    args = parser.parse_args()

    source_dir = args.sourcedir
    access_key = args.accesskey
    secret_key = args.secretkey
    bucket_name = args.bucket

    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    for file_name in os.listdir(source_dir):
        file_path = os.path.join(source_dir, file_name)

        if not os.path.isfile(file_path):
            print('Skipping directory: ' + file_path)
            continue

        with open(file_path, 'rb') as f:
            print('Transferring: ' + file_path)
            s3.upload_fileobj(f, bucket_name, file_name)

        print(' - Deleting...')
        os.remove(file_path)