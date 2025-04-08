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
import concurrent.futures

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

def process_file_wrapper(args):
    key, content = args
    process_file(key, content)


# def fetch_and_process_streaming(bucket_name, base_prefix, num_fetch_threads=10, num_processes=2, batch_size=10):
#     print("Fetching and processing files in streaming mode...")

#     keys = list_gz_files(bucket_name, base_prefix)

#     for i in range(0, len(keys), batch_size):
#         batch_keys = keys[i:i + batch_size]
#         results = {}

#         with ThreadPoolExecutor(max_workers=num_fetch_threads) as fetch_executor:
#             future_to_key = {
#                 fetch_executor.submit(fetch_log_file, bucket_name, key): key
#                 for key in batch_keys
#             }

#             for future in concurrent.futures.as_completed(future_to_key):
#                 key = future_to_key[future]
#                 try:
#                     content = future.result()
#                     if content:
#                         results[key] = content
#                 except Exception as e:
#                     print(f"Error fetching {key}: {e}")

#         if results:
#             with ProcessPoolExecutor(max_workers=num_processes) as process_executor:
#                 process_executor.map(process_file_wrapper, results.items())


def fetch_and_process_streaming(bucket_name, base_prefix, num_fetch_threads=10, num_processes=2, batch_size=10):
    print("Fetching and processing files in streaming mode...")
    keys = list_gz_files(bucket_name, base_prefix)

    for i in tqdm(range(0, len(keys), batch_size), desc="Batches", unit="batch"):
        batch_keys = keys[i:i + batch_size]
        results = {}

        with ThreadPoolExecutor(max_workers=num_fetch_threads) as fetch_executor:
            future_to_key = {
                fetch_executor.submit(fetch_log_file, bucket_name, key): key
                for key in batch_keys
            }

            for future in tqdm(concurrent.futures.as_completed(future_to_key), total=len(batch_keys), desc="Fetching", leave=False):
                key = future_to_key[future]
                try:
                    content = future.result()
                    if content:
                        results[key] = content
                except Exception as e:
                    print(f"Error fetching {key}: {e}")

        if results:
            print(f"Processing {len(results)} files...")
            with ProcessPoolExecutor(max_workers=num_processes) as process_executor:
                list(tqdm(
                    process_executor.map(process_file_wrapper, results.items()),
                    total=len(results),
                    desc="Processing",
                    leave=False
                ))

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
    base_prefix = "AWSLogs/375872583153/WAFLogs/cloudfront/honasa-mmrth-waf-prod/2025/04/05/"

    
    fetch_and_process_streaming(bucket_name, base_prefix)
    

if __name__ == '__main__':
    main()
