import os
import json
import time
import traceback
import pandas as pd

from datetime import datetime
from dateutil.tz import gettz

def transform_data(file_content, key):
    log_list = []
    try:
        file_content_split_list = file_content.split('\n')
        for each in range(0, len(file_content_split_list) - 2):
            isweb, host, referer, origin, user_agent, event_date, event_hour, timestamp, httpSourceId, action, httpSourceName, ja3Fingerprint, requestHeadersInserted, responseCodeSent, terminatingRuleId, terminatingRuleType, webaclId, clientIp, country, args, httpMethod, httpVersion, requestId, uri, rateBasedRule, ruleGroup, label_str = """""", """""", """""", """""", """""", """""", """""", """""", """""", """""", """""", """""", """""", """""", """""", """""", """""", """""", """""", """""", """""", """""", """""", """""", """""", """""", """"""
            event_year, event_month, event_day, event_minutes = None, None, None, None
            each_json = json.loads(file_content_split_list[each])
            if "timestamp" in each_json:
                timestamp_ = each_json["timestamp"]
                timestamp__ = datetime.fromtimestamp(timestamp_ / 1000, tz=gettz('Asia/Kolkata'))
                timestamp = timestamp__.strftime('%Y-%m-%d %H:%M:%S')
                event_hour = timestamp__.hour
                event_year = timestamp__.year
                event_month = timestamp__.month
                event_day = timestamp__.day
                event_minutes = timestamp__.minute
            if "httpSourceId" in each_json:
                httpSourceId = each_json["httpSourceId"].strip()
            if "action" in each_json:
                action = each_json["action"].strip()
            if "httpSourceName" in each_json:
                httpSourceName = each_json["httpSourceName"].strip()
            if "ja3Fingerprint" in each_json:
                ja3Fingerprint = each_json["ja3Fingerprint"].strip()
            if "terminatingRuleId" in each_json:
                terminatingRuleId = each_json["terminatingRuleId"].strip()
            if "httpRequest" in each_json:
                httpRequest = each_json["httpRequest"]
                if "clientIp" in httpRequest:
                    clientIp = httpRequest["clientIp"].strip()
                if "country" in httpRequest:
                    country = httpRequest["country"].strip()
                if "args" in httpRequest:
                    args = httpRequest["args"].strip()
                if "httpMethod" in httpRequest:
                    httpMethod = httpRequest["httpMethod"].strip()
                if "httpVersion" in httpRequest:
                    httpVersion = httpRequest["httpVersion"].strip()
                if "requestId" in httpRequest:
                    requestId = httpRequest["requestId"].strip()
                if "uri" in httpRequest:
                    uri = httpRequest["uri"].strip()
                if "headers" in httpRequest:
                    headers = httpRequest["headers"]
                    if type(headers) == list and headers:
                        for header in headers:
                            if header["name"].lower() == "host":
                                host = header["value"].strip()
                            if header["name"].lower() == "referer":
                                referer = header["value"].strip()
                            if header["name"].lower() == "origin":
                                origin = header["value"].strip()
                            if header["name"].lower() == "user-agent":
                                user_agent = header["value"].strip()
                            if header["name"].lower() == "isweb":
                                isweb = header["value"].strip()
            each_dict = {
                "event_time": timestamp,
                "http_source_id": httpSourceId,
                "action": action,
                "http_source_name": httpSourceName,
                "ja3_finger_print": ja3Fingerprint,
                "terminating_rule_id": terminatingRuleId,
                "client_ip": clientIp,
                "country": country,
                "args": args,
                "http_method": httpMethod,
                "http_version": httpVersion,
                "request_id": requestId,
                "uri": uri,
                "host": host,
                "referer": referer,
                "origin": origin,
                "user_agent": user_agent,
                "is_web": isweb,
                "event_year": event_year,
                "event_month": event_month,
                "event_day": event_day,
                "event_hour": event_hour,
                "event_minutes": event_minutes,
                "file_path": key
            }
            log_list.append(each_dict)
    except Exception as e:
        print(traceback.print_exc())
    print("Saved")
    return log_list


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
        partition_folder = f"{output_folder}/year={year}/month={month}/date={date}/hour={hour}/minute={minute}"
        os.makedirs(partition_folder, exist_ok=True)
        
        parquet_file = f"{partition_folder}/data_{int(time.time())}.parquet"
        logs[parquet_file] = group
    
    for file_path, data in logs.items():
        data.to_parquet(file_path, engine="pyarrow", index=False, compression="snappy")
        print(f"Saved: {file_path}")

    print(f"Data saved locally in Parquet format with partitions at {output_folder}")

