import boto3
import json
from globalVar import aws_access_key_id, aws_secret_access_key, REGION
from botocore.exceptions import ClientError

def get_aws_console():
    return boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

def get_cloudwatch_console():
    aws_console = get_aws_console()
    return aws_console.client(service_name="cloudwatch", region_name=REGION)



# -------------------------------------------------------------------------------------------------------------------------------------------------------

# Get Dashboard Body
def get_dashboard_body(cloudwatch_console, dashboard_name):
    try:
        response = cloudwatch_console.get_dashboard(DashboardName=dashboard_name)
        return json.loads(response['DashboardBody'])
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFound":
            print(f"Dashboard {dashboard_name} does not exist. Creating a new one.")
            return {"widgets": []}
        else:
            print(f"Unexpected error fetching dashboard {dashboard_name}: {e}")
            return {}


# Get Identifier for metric
def get_identifier(namespace):
    return {
        "AWS/Lambda": "FunctionName"
    }.get(namespace)

# Add or update metric widget
def add_metric_widget(dashboard_name, namespace, instance_id, metric_name, domain, stat="Sum", period=300):
    cloudwatch_console = get_cloudwatch_console()
    print(f"Adding {metric_name} widget for {instance_id} in {dashboard_name}")
    
    dashboard_body = get_dashboard_body(cloudwatch_console, dashboard_name)
    identifier = get_identifier(namespace)
    if not identifier:
        print(f"Unsupported namespace: {namespace}")
        return

    widget_found = False
    if "widgets" not in dashboard_body:
        print("Unexpected error fetching dashboard")
        return
    
    for wid in dashboard_body["widgets"]:
        if wid["properties"].get("title", "") == f"{metric_name}-{namespace}-{domain}":
            metrics = wid["properties"]["metrics"]
            if any(instance_id in metric for metric in metrics):
                print(f"{instance_id} already present in widget")
                return
            metrics.append([namespace, metric_name, identifier, instance_id])
            widget_found = True
            break

    if not widget_found:
        new_widget = {
            "height": 12,
            "width": 12,
            "y": len(dashboard_body["widgets"]) * 12,
            "x": 0,
            "type": "metric",
            "properties": {
                "title": f"{metric_name}-{namespace}-{domain}",
                "metrics": [[namespace, metric_name, identifier, instance_id]],
                "view": "timeSeries",
                "region": REGION,
                "stat": stat,
                "period": period
            }
        }
        dashboard_body["widgets"].append(new_widget)

    cloudwatch_console.put_dashboard(
        DashboardName=dashboard_name,
        DashboardBody=json.dumps(dashboard_body)
    )
    print(f"Dashboard {dashboard_name} updated successfully.")

# Update existing widgets to only include valid Lambda functions
def updateWidgetMetricInstances(dashboard_name, namespace, valid_instance_ids, metric_name, domain):
    cloudwatch_console = get_cloudwatch_console()
    try:
        response = cloudwatch_console.get_dashboard(DashboardName=dashboard_name)
        dashboard_body = json.loads(response['DashboardBody'])
    except ClientError as e:
        print(f"Error fetching dashboard: {e}")
        return

    updated = False
    for wid in dashboard_body["widgets"]:
        if wid["properties"].get("title", "") == f"{metric_name}-{namespace}-{domain}":
            old_metrics = wid["properties"]["metrics"]
            new_metrics = []

            for metric in old_metrics:
                if len(metric) >= 4 and metric[3] in valid_instance_ids:
                    new_metrics.append(metric)
                else:
                    print(f"Removing {metric} from widget")

            wid["properties"]["metrics"] = new_metrics
            updated = True

    if updated:
        cloudwatch_console.put_dashboard(
            DashboardName=dashboard_name,
            DashboardBody=json.dumps(dashboard_body)
        )
        print(f"Dashboard {dashboard_name} updated successfully.")



#-----------------------------------------------------------------------------------------------------------------------------------------------------------

def get_lambda_functions(tagList, domain):
    try:
        aws_console = get_aws_console()
        lambda_console = aws_console.client("lambda", region_name=REGION)
        paginator = lambda_console.get_paginator("list_functions")

        functions = []
        for page in paginator.paginate():
            # print(page)
            for function in page["Functions"]:
                function_name = function["FunctionName"]
                if any(tag in function_name for tag in tagList):
                    print(f"Lambda function matched: {function_name}")
                    functions.append(function_name)

        print(f"Total Lambda functions matched for {domain}: {len(functions)}")
        return functions

    except Exception as e:
        print(f"Error fetching Lambda functions: {e}")
        return []


# Metric configuration per AWS service
SERVICE_CONFIG = {
    "AWS/Lambda": {
        "fetch_func": get_lambda_functions,
        "metrics": [
            "Invocations",
            "Errors",
            "Duration",
            "Throttles",
            "ConcurrentExecutions"        
        ],
    }
}

# Process one namespace
def process_service(dashboard_name, namespace, tagList, domain):
    fetch_func = SERVICE_CONFIG[namespace]["fetch_func"]
    metrics = SERVICE_CONFIG[namespace]["metrics"]
    function_names = fetch_func(tagList, domain)

    for metric in metrics:
        updateWidgetMetricInstances(dashboard_name, namespace, function_names, metric, domain)

    for function_name in function_names:
        for metric in metrics:
            add_metric_widget(dashboard_name, namespace, function_name, metric, domain)




#------------------------------------------------------------ # Entrypoint # ------------------------------------------------------------------------------

def main():
    tagList, domain = ["mamaearth"], "mamaearth"
    dashboard_name = "honasa-mamaearth-lambda"

    for namespace in SERVICE_CONFIG:
        process_service(dashboard_name, namespace, tagList, domain)

main()
