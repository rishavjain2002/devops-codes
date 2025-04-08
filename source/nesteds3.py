import io
import os
import json
import time
import gzip
import boto3
import traceback
from udf import transform_data, save_data_as_parquet
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from credentials import region_name, aws_access_key_id, aws_secret_access_key, BUCKET_NAME
from tqdm import tqdm



server_path = os.path.dirname(os.path.realpath(__file__))
def get_aws_console():
    return boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

def get_sqs_console():
    return get_aws_console().client("sqs", region_name=region_name)

def get_s3_console():
    return get_aws_console().client("s3", region_name=region_name)


# -------------------------------------------------------------- Parallelism ------------------------------------------------------------

def fetch_files_parallel(bucket_name, base_prefix, num_threads=20):
    print("Fetching S3 files in parallel...")
    keys = list_gz_files(bucket_name, base_prefix)
    results = {}

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = {executor.submit(fetch_log_file, bucket_name, key): key for key in keys}
        for future, key in tqdm(futures.items(), desc="Downloading files"):
            try:
                content = future.result()
                if content:
                    results[key] = content
            except Exception as e:
                print(f"Exception while fetching {key}: {e}")

    return results

def process_helper(file_tuple):
    key, content = file_tuple
    process_file(key, content)

def process_files_parallel(files, num_processes=2):
    print(f"Processing {len(files)} files with multiprocessing...")
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        executor.map(process_helper, files.items())


# -------------------------------------------------------------- Functions ------------------------------------------------------------


def list_gz_files(bucket_name, base_prefix):
    s3 = get_s3_console()
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=base_prefix)
    key_list = []

    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".gz"):
                key_list.append(key)

    return key_list


def fetch_log_file(bucket_name, file_key):
    try:
        s3 = get_s3_console()
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        file_stream = response["Body"].read()

        if file_key.endswith(".gz"):
            with gzip.GzipFile(fileobj=io.BytesIO(file_stream), mode="rb") as f:
                log_content = f.read().decode("utf-8")
                print(file_key)
                return log_content
        return None
    except Exception as e:
        print(f"Error fetching {file_key}: {e}")
        return None


def upload_to_s3(local_folder, s3_folder):
    s3 = get_s3_console()
    for root, _, files in os.walk(local_folder):
        for file in files:
            local_path = os.path.join(root, file)
            s3_key = os.path.relpath(local_path, local_folder)
            s3_path = f"{s3_folder}/{s3_key}"

            try:
                s3.upload_file(local_path, BUCKET_NAME, s3_path)
                print(f"Uploaded {file} to s3://{BUCKET_NAME}/{s3_path}")
                os.remove(local_path)
            except Exception as e:
                print(f"Failed to upload {file}: {e}")


def process_file(file_key, file_content):
    try:
        print(f"Processing file: {file_key}")
        if file_content:
            result_list = transform_data(file_content, file_key)
            if result_list:
                print(f"Transformed {len(result_list)} records from {file_key}")
                output_path = os.path.join(server_path, "../data/me-waf-logs")
                save_data_as_parquet(result_list, output_folder=output_path)
                upload_to_s3(output_path, "me-waf-logs")
            else:
                print(f"No valid data found in {file_key}")
    except Exception:
        print("Error processing file")


def main():
    bucket_name = "aws-waf-logs-mmrth-prod"
    base_prefix = "AWSLogs/375872583153/WAFLogs/cloudfront/honasa-mmrth-waf-prod/2025/04/06/"

    
    files = fetch_files_parallel(bucket_name, base_prefix)
    if files:
        print("Files fetched. Starting processing...")
        process_files_parallel(files)
    else:
        print("No new files found.")
    

if __name__ == '__main__':
    main()
