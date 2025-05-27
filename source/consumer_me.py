import io
import os
import json
import time
import gzip
import boto3
import traceback
from udf import transform_data, save_data_as_parquet
from credentials import region_name, aws_access_key_id, aws_secret_access_key, BATCH_SIZE, BUCKET_NAME, queue_url_me


server_path = os.path.dirname(os.path.realpath(__file__))
def get_aws_console():
    aws_console = boto3.Session(
        aws_access_key_id= aws_access_key_id,
        aws_secret_access_key= aws_secret_access_key
    )
    return aws_console

def get_sqs_console():
    aws_console = get_aws_console()
    sqs_console = aws_console.client(service_name = "sqs", region_name = region_name)
    return sqs_console

def get_s3_console():
    aws_console = get_aws_console()
    s3_console = aws_console.client(service_name = "s3", region_name = region_name)
    return s3_console



# -------------------------------------------------------------------------------------------------------------------------------------------

def receive_messages(queue_url, max_messages=10, wait_time=10):
    try:
        sqs_client = get_sqs_console()
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time,
            MessageAttributeNames=['All'],
        )
        return response.get("Messages", [])  # Return an empty list if no messages
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

def upload_to_s3(local_folder, s3_folder):
    s3 = get_s3_console()
    for root, dirs, files in os.walk(local_folder):
        for file in files:
            local_file_path = os.path.join(root, file)
            s3_key = os.path.relpath(local_file_path, local_folder)
            s3_path = f"{s3_folder}/{s3_key}"
            s3.upload_file(local_file_path, BUCKET_NAME, s3_path)
            print(f"Uploaded {local_file_path} to s3://{BUCKET_NAME}/{s3_path}")
            os.remove(local_file_path)
    

def process_file(bucket_name, file_key):
    try:
        print("trying to get file")
        file_content = fetch_log_file(bucket_name, file_key)
        print(type(file_content))
        if file_content:
            print("Got File Data !!!")
            result_list = transform_data(file_content, file_key)
            if len(result_list) > 0:
                print("Found {} rows".format(len(result_list)))
                for i in range(0, len(result_list), BATCH_SIZE):
                    print("Inserting data for Batch Size from {} to {}".format(i, i + BATCH_SIZE))
                    batch = result_list[i:i + BATCH_SIZE]
                    path_to_file = os.path.join(server_path, "../data/me-waf-logs")
                    print(path_to_file)
                    save_data_as_parquet(batch, output_folder=path_to_file)
                    upload_to_s3(path_to_file, "me-waf-logs")
        
    
    except Exception as e:
        print(traceback.print_exc())
        print(f"Error in processing data and saving data as parquet: {e}")
        

def main():
    print("Fetching all files")
    queue_url = queue_url_me
    sqs_console = get_sqs_console()
    while True:
        messages = receive_messages(queue_url)
        if not messages:
            print("No messages available. Waiting...")
            time.sleep(5)
            continue

        for message in messages:
            try:
                bucket_name, file_key = getBucketandKey(message)
                if bucket_name and file_key:
                    process_file(bucket_name, file_key)

                receipt_handle = message["ReceiptHandle"]
                sqs_console.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
                print("message deleted")

            except Exception as e:
                print(traceback.print_exc())
                print(f"Error processing message: {e}")

        time.sleep(1)
    
if __name__ == '__main__':
    main()
