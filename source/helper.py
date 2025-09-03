import boto3
from credentials import aws_access_key_id, aws_secret_access_key, REGION, LOG_GROUP, query_string, SLACK_WEBHOOK_URL
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone
import time
import json
import requests

def get_aws_console():
    aws_console = boto3.Session(
        aws_access_key_id= aws_access_key_id,
        aws_secret_access_key= aws_secret_access_key
    )
    return aws_console


def get_cloudwatchLogs_console():
    aws_console = get_aws_console()
    cloudwatchlogs_console = aws_console.client(service_name = "logs", region_name = REGION)
    return cloudwatchlogs_console


# -------------------------------------------------------------------------------------------------------------------

def send_slack_alert(pod_name, namespace, container_name, restart_count, timestamp):
    message = {
        "text": f"🚨 *Pod Restart Detected!*",
        "attachments": [
            {
                "color": "#ff0000",
                "fields": [
                    {"title": "Pod", "value": pod_name, "short": True},
                    {"title": "Namespace", "value": namespace, "short": True},
                    {"title": "Container", "value": container_name, "short": True},
                    {"title": "Restart Count", "value": str(restart_count), "short": True},
                    {"title": "Time", "value": timestamp, "short": False}
                ]
            }
        ]
    }
    response = requests.post(SLACK_WEBHOOK_URL, json=message)
    if response.status_code != 200:
        print(f"Slack Error: {response.status_code} - {response.text}")


def run_query():
    cloudwatchlogs_console = get_cloudwatchLogs_console()
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=50)

    start_query_response = cloudwatchlogs_console.start_query(
        logGroupName=LOG_GROUP,
        startTime=int(start_time.timestamp() * 1000),
        endTime=int(end_time.timestamp() * 1000),
        queryString=query_string
    )

    query_id = start_query_response['queryId']

    while True:
        response = cloudwatchlogs_console.get_query_results(queryId=query_id)
        if response['status'] == 'Complete':
            break
        time.sleep(1)

    for result in response['results']:
        message = None
        timestamp_str = None
        for field in result:
            if field['field'] == '@timestamp':
                timestamp_str = field['value']

            if field['field'] == '@message':
                message = json.loads(field['value'])
                break

        if not message:
            continue

        pod_name = message.get('objectRef', {}).get('name', 'N/A')
        namespace = message.get('objectRef', {}).get('namespace', 'N/A')
        container_statuses = (
            message.get('requestObject', {}).get('status', {}).get('containerStatuses', [])
        )

        for container in container_statuses:
            restart_count = container.get('restartCount', 0)
            container_name = container.get('name', 'N/A')
            if restart_count > 0:
                print(restart_count)
                print(f"Sending Slack alert for Pod: {pod_name}, Restart Count: {restart_count}")
                send_slack_alert(pod_name, namespace, container_name, restart_count, timestamp_str)











