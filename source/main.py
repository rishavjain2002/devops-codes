import boto3
from credentials import aws_access_key_id, aws_secret_access_key, REGION, spot_event_queue, SLACK_WEBHOOK_URL
import traceback
import time
import json
from helper import prepare_mail_body
from datetime import datetime, timedelta, timezone
import requests



aws_console = boto3.Session(
    aws_access_key_id= aws_access_key_id,
    aws_secret_access_key= aws_secret_access_key
)

sqs_client = aws_console.client(service_name='sqs', region_name=REGION)
ec2_client = aws_console.client(service_name='ec2', region_name=REGION)
cloudwatch_client = aws_console.client(service_name='cloudwatch', region_name=REGION)
ssm_client = aws_console.client(service_name='ssm', region_name=REGION)

# ------------------------------------------------------------------------ Helper Functions -------------------------------------------------------------------------------------

def receive_messages(queue_url, max_messages=10, wait_time=10):
    try:
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


def get_cluster_name(instance_id):
    try:
        response = ec2_client.describe_tags(
            Filters=[
                {"Name": "resource-id", "Values": [instance_id]},
                {"Name": "key", "Values": ["aws:eks:cluster-name"]}
            ]
        )
        tags = response.get("Tags", [])
        if tags:
            return tags[0]["Value"]
        return "UnknownCluster"
    except Exception as e:
        print(traceback.print_exc())
        print(f"Error fetching tags for {instance_id}: {e}")
        return "UnknownCluster"
    

def put_spot_metric(cluster_name):
    try:
        cloudwatch_client.put_metric_data(
            Namespace="Custom/SpotTermination",
            MetricData=[
                {
                    "MetricName": "TerminationEvent",
                    "Value": 1,
                    "Unit": "Count",
                    "Dimensions": [
                        {"Name": "ClusterName", "Value": cluster_name}
                    ]
                }
            ]
        )
        print(f"Metric published for cluster: {cluster_name}")
    except Exception as e:
        print(traceback.print_exc())
        print(f"Error publishing metric: {e}")


def get_pods_on_instance(instance_id):
    try:
        response = ssm_client.send_command(
            InstanceIds=[instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={
                "commands": [
                    # This will return pods running on that node
                    "kubectl get pods --all-namespaces -o wide | grep $(curl -s http://169.254.169.254/latest/meta-data/instance-id)"
                ]
            },
        )

        command_id = response["Command"]["CommandId"]

        # Wait and fetch output
        time.sleep(3)
        output = ssm_client.get_command_invocation(
            CommandId=command_id,
            InstanceId=instance_id,
        )

        if output["Status"] == "Success":
            print(f"Pods running on instance {instance_id}:\n{output['StandardOutputContent']}")
            return output["StandardOutputContent"]
        else:
            print(f"SSM command failed: {output['Status']}")
            return None

    except Exception as e:
        print(traceback.print_exc())
        print(f"Error fetching pods for {instance_id}: {e}")
        return None


def send_spot_interruption_slack(instance_id, cluster_name, pods):
    IST = timezone(timedelta(hours=5, minutes=30))
    timestamp = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")

    pod_list = pods.splitlines() if pods else []
    pod_text = "\n".join([f"• {p}" for p in pod_list]) if pod_list else "No pods found."

    message = {
        "text": "🚨 *Spot Instance Interruption Detected!*",
        "attachments": [
            {
                "color": "#ff0000",
                "fields": [
                    {"title": "Cluster", "value": cluster_name, "short": True},
                    {"title": "Instance ID", "value": instance_id, "short": True},
                    {"title": "Time", "value": timestamp, "short": True},
                    {"title": "Pods on Node", "value": pod_text, "short": False},
                ]
            }
        ]
    }

    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=message)
        if response.status_code != 200:
            print(f"Slack Error: {response.status_code} - {response.text}")
        else:
            print("Slack alert sent successfully")
    except Exception as e:
        print(f"Error sending Slack alert: {e}")


def sesMail(report_body, sender, receiver):
    try:
        ses_client = aws_console.client("ses", region_name=REGION)
        ses_client.send_email(
            Source=sender,
            Destination={'ToAddresses': [receiver]},
            Message={
                'Subject': {'Data': f'EC2 Instance Spot Interruption - {datetime.now().strftime("%d-%m-%Y")}'},
                'Body': {
                    'Html': {'Data': report_body}
                }
            }
        )
        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")


sender_email = "techadmin@mamaearth.in"
receiver_email = ["rishav.jain@mamaearth.in", "nikhil.m@mamaearth.in", "naman.v@mamaearth.in"]


# ------------------------------------------------------------------------ Main Functions -------------------------------------------------------------------------------------


def main():
    print("Fetching all files")
    queue_url = spot_event_queue
    while True:
        messages = receive_messages(queue_url)
        if not messages:
            print("No messages available. Waiting...")
            time.sleep(5)
            continue

        for msg in messages:
            try:
                body = json.loads(msg["Body"])
                if "Message" in body:
                    body = json.loads(body["Message"])

                instance_id = body["detail"]["instance-id"]
                cluster_name = get_cluster_name(instance_id)

                # Publish metric
                put_spot_metric(cluster_name)

                # Get pod running in the instance
                pods = get_pods_on_instance(instance_id)
                if pods:
                    print(f"Found pods:\n{pods}")

                # Notification
                mail_body = prepare_mail_body(instance_id, cluster_name, pods)
                for receiver in receiver_email:
                    sesMail(mail_body, sender_email, receiver)
                # send_spot_interruption_slack(instance_id, cluster_name, pods)
                
                # Delete the message
                receipt_handle = msg["ReceiptHandle"]
                sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
                print(f"Deleted message with ReceiptHandle: {receipt_handle}")

            except Exception as e:
                print(traceback.print_exc())
                print(f"Error processing message: {e}")

        time.sleep(1)

if __name__ == "__main__":

    # send_spot_interruption_slack("i-7y93y983703", "eks-test-cluster", ["pod1", "pod2"])
    # put_spot_metric("")
    main()
