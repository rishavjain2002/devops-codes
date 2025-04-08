import boto3
import traceback
from credentials import aws_access_key_id, aws_secret_access_key, SNS_TOPIC_ARN, REGION

def get_aws_console():
    aws_console = boto3.Session(
        aws_access_key_id= aws_access_key_id,
        aws_secret_access_key= aws_secret_access_key
    )
    return aws_console


def get_cloudwatch_console():
    aws_console = get_aws_console()
    cloudwatch_console = aws_console.client(service_name = "cloudwatch", region_name = REGION)
    return cloudwatch_console

def get_ec2_console():
    aws_console = get_aws_console()
    ec2_console = aws_console.client(service_name = "ec2", region_name = REGION)
    return ec2_console


# --------------------------------- Main Functions ---------------------------------

def getInstanceId():
    try:
        ec2_console = get_ec2_console()
        running_instances = []
        response = ec2_console.describe_instances(
            Filters=[
                {
                    'Name': 'instance-state-name',
                    'Values': [
                        'running',
                    ]
                },
            ],
        )

        count = 0
        for res in response.get("Reservations", []):
            for inst in res.get("Instances", []):
                instance_id = inst["InstanceId"]
                count += 1
                print(instance_id)
                running_instances.append(instance_id)
        print(count)
        return running_instances

    except Exception as e:
        traceback.print_exc()
        print(f"Error fetching ec2 instaces:: {e}")
        return []



def deleteAlarm(running_instances):
    try:
        cloudwatch_console = get_cloudwatch_console()
        alarms_to_delete = []
        response = cloudwatch_console.describe_alarms()

        for alarm in response["MetricAlarms"]:
            instance_id = None
            for dimension in alarm["Dimensions"]:
                if dimension["Name"] == "InstanceId":
                    instance_id = dimension["Value"]
                    break
            
            if instance_id:
                if instance_id not in running_instances:
                    alarms_to_delete.append(alarm["AlarmName"])


        if alarms_to_delete:
            cloudwatch_console.delete_alarms(AlarmNames=alarms_to_delete)
            print(f"Deleted alarms: {alarms_to_delete}")
        else:
            print("No alarms to delete.")

    except Exception as e:
        traceback.print_exc()
        print("Error in deleteAlarm function")


def createhealthCheckAlarm(instanceId, metric_name, alarm_name):
    try:
        cloudwatch_console = get_cloudwatch_console()

        def checkAlarm(alarm_name):
            response = cloudwatch_console.describe_alarms(
                AlarmNames=[
                    alarm_name
                ]
            )
            return len(response["MetricAlarms"]) > 0
        
        if checkAlarm(alarm_name):
            print(f"{alarm_name} already exists")
            return 
        
        print(f"Setting up {metric_name} alarm for {instanceId}")
        cloudwatch_console.put_metric_alarm(
            AlarmName=alarm_name,
            MetricName=metric_name,
            Namespace="AWS/EC2",
            Statistic="Maximum",
            Period=300,  
            EvaluationPeriods=2,
            Threshold=1,
            ComparisonOperator="GreaterThanOrEqualToThreshold",
            Dimensions=[{"Name": "InstanceId", "Value": instanceId}],
            AlarmActions=[SNS_TOPIC_ARN],
            Unit="Count",
            AlarmDescription=f"Alarm for {metric_name} on {instanceId}",
        )
        print(f"Alarm Created for {instanceId}")
    
    except Exception as e:
        traceback.print_exc()
        print(f"Error in creating alarm {alarm_name} of {instanceId}")
