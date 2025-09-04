# -------------------------------------------------------------- MultiProcessing Imports --------------------------------------------------------------
from multiprocessing import Process, current_process, set_start_method, Manager, Value
import signal
import sys

# -------------------------------------------------------------- Main Imports --------------------------------------------------------------
import io
import os
import json
import time
import gzip
import boto3
from botocore.exceptions import BotoCoreError, ClientError
import traceback
from udf import transform_data, save_data_as_parquet
from credentials import region_name, aws_access_key_id, aws_secret_access_key, BATCH_SIZE, BUCKET_NAME, queue_url_tdc

server_path = os.path.dirname(os.path.realpath(__file__))

def get_aws_console():
    aws_console = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    return aws_console

def get_sqs_console():
    aws_console = get_aws_console()
    sqs_console = aws_console.client(service_name="sqs", region_name=region_name)
    return sqs_console

def get_s3_console():
    aws_console = get_aws_console()
    s3_console = aws_console.client(service_name="s3", region_name=region_name)
    return s3_console

# -------------------------------------------------------------- Helper Functions --------------------------------------------------------------

def receive_messages(queue_url, max_messages=5, wait_time=10):
    try:
        sqs_client = get_sqs_console()
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time,
            MessageAttributeNames=['All']
        )
        messages = response.get("Messages", [])
        for m in messages:
            print(f"[DEBUG] Fetched MessageId={m['MessageId']} "
                  f"ReceiptHandle={m['ReceiptHandle'][:20]}... by {current_process().name}")
        return messages
    except Exception as e:
        print(traceback.print_exc())
        print(f"Error receiving messages from SQS: {e}")
        return []

def getBucketandKey(message):
    try:
        message_body = json.loads(message["Body"])
        record = message_body["Records"][0]
        if "s3" in record:
            return record["s3"]["bucket"]["name"], record["s3"]["object"]["key"]
        else:
            print("Invalid S3 record in message.")
            return None, None
    except Exception as e:
        print(traceback.print_exc())
        print(f"Error extracting bucket details: {e}")
        return None, None

def fetch_log_file(bucket_name, file_key):
    try:
        s3_console = get_s3_console()
        response = s3_console.get_object(Bucket=bucket_name, Key=file_key)
        file_stream = response["Body"].read()

        if file_key.endswith(".gz"):
            with gzip.GzipFile(fileobj=io.BytesIO(file_stream), mode="rb") as f:
                log_content = f.read().decode("utf-8")
                return log_content
        else:
            print("File format not supported.")
            return None
    except Exception as e:
        print(traceback.print_exc())
        print(f"Error fetching log file: {e}")
        return None

def upload_to_s3(local_folder, s3_folder, max_retries=3, retry_delay=2):
    s3 = get_s3_console()
    try:
        for root, dirs, files in os.walk(local_folder):
            for file in files:
                print(local_folder)
                print("Uploading file:")
                print(file)
                local_file_path = os.path.join(root, file)
                print(local_file_path)
                s3_key = os.path.relpath(local_file_path, local_folder)
                print(s3_key)
                s3_path = f"{s3_folder}/{s3_key}"

                s3.upload_file(local_file_path, BUCKET_NAME, s3_path)
                print(f"Uploaded {local_file_path} to s3://{BUCKET_NAME}/{s3_path}")
                os.remove(local_file_path)
    except Exception as e:
        print(f"Error uploading files to S3: {e}")
def process_file(bucket_name, file_key):
    try:
        print("trying to get file")
        file_content = fetch_log_file(bucket_name, file_key)
        print(type(file_content))
        if file_content:
            print("Got File Data !!!")
            cf_id = file_key.split("/")[1].split(".")[0]
            print(cf_id)
            result_list = transform_data(file_content, file_key, cf_id)
            if len(result_list) > 0:
                print("Found {} rows".format(len(result_list)))
                for i in range(0, len(result_list), BATCH_SIZE):
                    print("Inserting data for Batch Size from {} to {}".format(i, i + BATCH_SIZE))
                    batch = result_list[i:i + BATCH_SIZE]
                    path_to_file = os.path.join(server_path, "../data/tdc-cf-logs")
                    print(path_to_file)
                    save_data_as_parquet(batch, output_folder=path_to_file)
                    upload_to_s3(path_to_file, "me-cf-logs")
    except Exception as e:
        print(traceback.print_exc())
        print(f"Error in processing data and saving data as parquet: {e}")

# -------------------------------------------------------------- Worker Function --------------------------------------------------------------
def worker(seen_messages, max_time_value, terminate_flag):
    print(f"[{current_process().name}] Starting worker process")
    queue_url = queue_url_tdc
    sqs_console = get_sqs_console()

    while not terminate_flag.value:
        messages = receive_messages(queue_url)
        if not messages:
            print(f"[{current_process().name}] No messages available. Waiting...")
            time.sleep(5)
            continue

        for message in messages:
            if terminate_flag.value:
                break  # another worker already triggered termination

            msg_id = message["MessageId"]
            if msg_id in seen_messages:
                print(f"[WARNING] Duplicate fetch! "
                      f"MessageId={msg_id} already fetched by {seen_messages[msg_id]}, "
                      f"now fetched by {current_process().name}")
                
                terminate_flag.value = True  # trigger termination
                os.kill(os.getppid(), signal.SIGINT)  # send signal to main process
                break

            seen_messages[msg_id] = current_process().name

            try:
                start_time = time.time()

                bucket_name, file_key = getBucketandKey(message)
                print("BUCKET")
                print(bucket_name)
                receipt_handle = message["ReceiptHandle"]
                sqs_console.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

                print(f"[{current_process().name}] Processing file: {file_key}")
                if bucket_name and file_key:
                    process_file(bucket_name, file_key)

                

                end_time = time.time()
                duration = end_time - start_time
                print(f"[{current_process().name}] MessageId={msg_id} processed in {duration:.2f} sec")

                with max_time_value.get_lock():
                    if duration > max_time_value.value:
                        max_time_value.value = duration
                        print(f"[{current_process().name}] New MAX processing time = {duration:.2f} sec")

                print(f"[{current_process().name}] Message deleted "
                      f"(MessageId={msg_id}, ReceiptHandle={receipt_handle[:20]}...)")

            except Exception as e:
                print(traceback.print_exc())
                print(f"[{current_process().name}] Error processing message: {e}")

        time.sleep(1)

# -------------------------------------------------------------- MultiProcessing --------------------------------------------------------------
def main():
    try:
        set_start_method("spawn", force=True)
    except RuntimeError:
        pass

    num_workers = 4
    processes = []

    manager = Manager()
    seen_messages = manager.dict()
    max_time_value = Value('d', 0.0)
    terminate_flag = manager.Value('b', False)  # shared boolean flag

    def signal_handler(sig, frame):
        print("Stopping all workers...")
        for p in processes:
            p.terminate()
        print(f"Maximum processing time observed = {max_time_value.value:.2f} sec")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    for i in range(num_workers):
        p = Process(target=worker, args=(seen_messages, max_time_value, terminate_flag), name=f"Worker-{i+1}")
        p.start()
        processes.append(p)

    for p in processes:
        p.join()


if __name__ == '__main__':
    main()
