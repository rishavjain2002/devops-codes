import os
import time
import shlex
import traceback
import pandas as pd

from datetime import datetime

def transform_data(log_content, prefix):
    result_list = []
    field_name = ["type", "event_time", "elb", "client_port", "target_port", "request_processing_time",
                 "target_processing_time", "response_processing_time", "elb_status_code", "target_status_code",
                 "received_bytes", "sent_bytes", "request", "user_agent", "ssl_cipher", "ssl_protocol",
                 "target_group_arn", "trace_id", "domain_name", "chosen_cert_arn", "matched_rule_priority",
                 "request_creation_time", "actions_executed", "redirect_url", "error_reason", "target_port_list",
                 "target_status_code_list", "classification", "classification_reason", "conn_trace_id"
                 ]
    try:
        for log in log_content.split("\n"): 
            log_data = shlex.split(log)

            if not log.strip():
                print("Skipping empty Line")
                continue
            
            if len(log_data) != len(field_name):
                print(f"Skipping line with incorrect format: {log}")
                continue

            data_dict = dict(zip(field_name, log_data))
            data_dict["event_time"] = datetime.strptime(data_dict["event_time"], "%Y-%m-%dT%H:%M:%S.%fZ")
            data_dict["event_time"] = data_dict["event_time"].strftime("%Y-%m-%d %H:%M:%S")
            data_dict["prefix"] = prefix
            result_list.append(data_dict)
    except Exception as e:
        print(traceback.print_exc())
    return result_list

def save_data_as_parquet(batch, output_folder):
    df = pd.DataFrame(batch)
    df["event_time"] = pd.to_datetime(df["event_time"])
    df["event_minutes"] = (df["event_time"].dt.minute // 5) * 5
    df["event_year"] = df["event_time"].dt.year
    df["event_month"] = df["event_time"].dt.month
    df["event_day"] = df["event_time"].dt.day
    df["event_hour"] = df["event_time"].dt.hour
    df["event_time"] = df["event_time"].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    logs = {}
    
    for (year, month, date, hour, minute), group in df.groupby(["event_year", "event_month", "event_day", "event_hour", "event_minutes"]):
        partition_folder = f"{output_folder}/year={year}/month={month}/date={date}/hour={hour}/minutes={minute}"
        os.makedirs(partition_folder, exist_ok=True)
        
        parquet_file = f"{partition_folder}/data_{int(time.time())}.parquet"
        logs[parquet_file] = group
    
    for file_path, data in logs.items():
        data.to_parquet(file_path, engine="pyarrow", index=False, compression="snappy")
        print(f"Saved: {file_path}")

    print(f"Data saved locally in Parquet format with partitions at {output_folder}")
