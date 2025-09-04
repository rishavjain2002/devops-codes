import os
import time
import traceback
import pandas as pd
import sys
import shlex
from datetime import datetime
import traceback



def transform_data(log_content, prefix, cf_id):
    result_list = []
    try:
        for log in log_content.split("\n"):
            line_splitted_list = log.split("\t")
            if len(line_splitted_list) >= 33:
                date_time, event_hour, event_year, event_month, event_day, event_minutes = None, None, None, None, None, None
                date_time_str = line_splitted_list[0].strip().rstrip().lstrip() + " " + line_splitted_list[1].strip().rstrip().lstrip()
                if date_time_str:
                    date_time_ = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
                    event_hour = date_time_.hour
                    event_year = date_time_.year
                    event_month = date_time_.month
                    event_day = date_time_.day
                    event_minutes = date_time_.minute
                    date_time = date_time_.strftime('%Y-%m-%d %H:%M:%S')
                x_edge_location = line_splitted_list[2].strip().rstrip().lstrip()
                sc_bytes = line_splitted_list[3].strip().rstrip().lstrip()
                c_ip = line_splitted_list[4].strip().rstrip().lstrip()
                cs_method = line_splitted_list[5].strip().rstrip().lstrip()
                cs_host = line_splitted_list[6].strip().rstrip().lstrip()
                cs_uri_stem = line_splitted_list[7].strip().rstrip().lstrip()
                sc_status = line_splitted_list[8].strip().rstrip().lstrip()
                cs_referer = line_splitted_list[9].strip().rstrip().lstrip()
                cs_user_agent = line_splitted_list[10].strip().rstrip().lstrip()
                cs_uri_query = line_splitted_list[11].strip().rstrip().lstrip()
                cs_cookie = line_splitted_list[12].strip().rstrip().lstrip()
                x_edge_result_type = line_splitted_list[13].strip().rstrip().lstrip()
                x_edge_request_id = line_splitted_list[14].strip().rstrip().lstrip()
                x_host_header = line_splitted_list[15].strip().rstrip().lstrip()
                cs_protocol = line_splitted_list[16].strip().rstrip().lstrip()
                cs_bytes = line_splitted_list[17].strip().rstrip().lstrip()
                time_taken = line_splitted_list[18].strip().rstrip().lstrip()
                x_forwarded_for = line_splitted_list[19].strip().rstrip().lstrip()
                ssl_protocol = line_splitted_list[20].strip().rstrip().lstrip()
                ssl_cipher = line_splitted_list[21].strip().rstrip().lstrip()
                x_edge_response_result_type = line_splitted_list[22].strip().rstrip().lstrip()
                cs_protocol_version = line_splitted_list[23].strip().rstrip().lstrip()
                fle_status = line_splitted_list[24].strip().rstrip().lstrip()
                fle_encrypted_fields = line_splitted_list[25].strip().rstrip().lstrip()
                c_port = line_splitted_list[26].strip().rstrip().lstrip()
                time_to_first_byte = line_splitted_list[27].strip().rstrip().lstrip()
                x_edge_detailed_result_type = line_splitted_list[28].strip().rstrip().lstrip()
                sc_content_type = line_splitted_list[29].strip().rstrip().lstrip()
                sc_content_len = line_splitted_list[30].strip().rstrip().lstrip()
                sc_range_start = line_splitted_list[31].strip().rstrip().lstrip()
                sc_range_end = line_splitted_list[32].strip().rstrip().lstrip()
                if date_time and event_hour and event_year and event_month and event_day and event_minutes:
                    dict_ = {
                        "event_time": date_time,
                        "x_edge_location": x_edge_location,
                        "sc_bytes": sc_bytes,
                        "c_ip": c_ip,
                        "cs_method": cs_method,
                        "cs_host": cs_host,
                        "cs_uri_stem": cs_uri_stem,
                        "sc_status": sc_status,
                        "cs_referer": cs_referer,
                        "cs_user_agent": cs_user_agent,
                        "cs_uri_query": cs_uri_query,
                        "cs_cookie": cs_cookie,
                        "x_edge_result_type": x_edge_result_type,
                        "x_edge_request_id": x_edge_request_id,
                        "x_host_header": x_host_header,
                        "cs_protocol": cs_protocol,
                        "cs_bytes": cs_bytes,
                        "time_taken": time_taken,
                        "x_forwarded_for": x_forwarded_for,
                        "ssl_protocol": ssl_protocol,
                        "ssl_cipher": ssl_cipher,
                        "x_edge_response_result_type": x_edge_response_result_type,
                        "cs_protocol_version": cs_protocol_version,
                        "fle_status": fle_status,
                        "fle_encrypted_fields": fle_encrypted_fields,
                        "c_port": c_port,
                        "time_to_first_byte": time_to_first_byte,
                        "x_edge_detailed_result_type": x_edge_detailed_result_type,
                        "sc_content_type": sc_content_type,
                        "sc_content_len": sc_content_len,
                        "sc_range_start": sc_range_start,
                        "sc_range_end": sc_range_end,
                        "cloud_front_id": cf_id,
                        "event_year": event_year,
                        "event_month": event_month,
                        "event_day": event_day,
                        "event_hour": event_hour,
                        "event_minutes": event_minutes,
                        "prefix": prefix
                    }
                    result_list.append(dict_)
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
    
    for (cloud_front_id, year, month, date, hour, minute), group in df.groupby(["cloud_front_id", "event_year", "event_month", "event_day", "event_hour", "event_minutes"]):

        partition_folder = f"{output_folder}/cf_id={cloud_front_id}/year={year}/month={month}/date={date}/hour={hour}/minutes={minute}"
        os.makedirs(partition_folder, exist_ok=True)
        
        parquet_file = f"{partition_folder}/data_{int(time.time())}.parquet"
        logs[parquet_file] = group
    
    for file_path, data in logs.items():
        data.to_parquet(file_path, engine="pyarrow", index=False, compression="snappy")
        print(f"Saved: {file_path}")

    print(f"Data saved locally in Parquet format with partitions at {output_folder}")


# for (year, month), group in df.groupby(["year", "month"]):
#     path = f"s3://your-bucket/sales-data/year={year}/month={month}/sales_{year}_{month}.parquet"
#     group.to_parquet(
#         path,
#         engine="pyarrow",
#         index=False,
#         storage_options={...}
#     )
