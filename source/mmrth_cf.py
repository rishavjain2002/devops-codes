import boto3
import json
from globalVar import aws_access_key_id, aws_secret_access_key, REGION
import traceback
from botocore.exceptions import ClientError

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


#---------------------------------------------------------------- Main Functions -----------------------------------------------------------------------

def get_dashboard_body(cloudwatch_console, dashboard_name):
    cloudwatch_console = get_cloudwatch_console()
    try:
        response = cloudwatch_console.get_dashboard(DashboardName=dashboard_name)
        return json.loads(response['DashboardBody'])
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFound":
            print(f"Dashboard {dashboard_name} does not exist. Creating a new one.")
            print(f"Blank Dashboard {dashboard_name} created successfully.")
            return {"widgets": []}

        else:
            print(f"Unexpected error fetching dashboard {dashboard_name}: {e}")


def get_identifier(namespace):
    return {
        "AWS/CloudFront": "DistributionId",
    }.get(namespace)


def add_metric_widget(dashboard_name, namespace, instance_id, metric_name, micro_service, stat="Sum", period=300):
    cloudwatch_console = get_cloudwatch_console()
    print(f"Adding {metric_name} widget for {namespace} instance ID {instance_id} in {dashboard_name}")
    
    dashboard_body = get_dashboard_body(cloudwatch_console, dashboard_name)
    identifier = get_identifier(namespace)
    if not identifier:
        print(f"Unsupported namespace: {namespace}")
        return

    widget_found = False
    for wid in dashboard_body["widgets"]:
        widgetTitle = wid["properties"].get("title", "")
        if widgetTitle == f"{metric_name}-{namespace}-{micro_service}":
            print(f"Checking existing widget for {namespace}")
            metrics = wid["properties"]["metrics"]
            if any(instance_id in m for m in metrics if isinstance(m, list)):
                print(f"Instance {instance_id} already present")
                return
            print(f"Adding new instance {instance_id} to {namespace}")
            metrics.append([namespace, metric_name, identifier, instance_id])
            widget_found = True
            break

    if not widget_found:
        print(f"No existing widget found for {namespace}. Creating a new widget.")
        new_widget = create_widget_template(metric_name, namespace, instance_id, identifier, micro_service, stat, period)
        dashboard_body["widgets"].append(new_widget)

    cloudwatch_console.put_dashboard(
        DashboardName=dashboard_name,
        DashboardBody=json.dumps(dashboard_body)
    )
    print(f"Dashboard {dashboard_name} updated successfully.")


def create_widget_template(metric_name, namespace, instance_id, identifier, domain, stat, period):
    common_props = {
        "view": "timeSeries",
        "stacked": False,
        "region": "us-east-1",
        "stat": stat,
        "period": period,
        "yAxis": {
            "left": {"showUnits": False},
            "right": {"showUnits": False}
        },
        "title": f"{metric_name}-{namespace}-{domain}",
    }

    base_metrics = {
        "Requests": [[namespace, "Requests", "Region", "Global", identifier, instance_id]],
        "DataTransfer": [
            [namespace, "BytesUploaded", "Region", "Global", identifier, instance_id],
            [namespace, "BytesDownloaded", "Region", "Global", identifier, instance_id]
        ],
        "TotalErrorRate": [
            [namespace, "TotalErrorRate", "Region", "Global", identifier, instance_id],
            [namespace, "4xxErrorRate", "Region", "Global", identifier, instance_id, {"label": "Total4xxErrors"}],
            [namespace, "5xxErrorRate", "Region", "Global", identifier, instance_id, {"label": "Total5xxErrors"}],
            [{"expression": "(m4+m5+m6)/m7*100", "label": "5xxErrorByLambdaEdge", "id": "e1"}],
            [namespace, "LambdaExecutionError", "Region", "Global", identifier, instance_id, {"id": "m4", "stat": "Sum", "visible": False}],
            [namespace, "LambdaValidationError", "Region", "Global", identifier, instance_id, {"id": "m5", "stat": "Sum", "visible": False}],
            [namespace, "LambdaLimitExceededError", "Region", "Global", identifier, instance_id, {"id": "m6", "stat": "Sum", "visible": False}],
            [namespace, "Requests", "Region", "Global", identifier, instance_id, {"id": "m7", "stat": "Sum", "visible": False}]
        ],
        "4xxErrorRate": [
            [namespace, "4xxErrorRate", "Region", "Global", identifier, instance_id, {"label": "Total4xxErrors"}],
            [namespace, "401ErrorRate", "Region", "Global", identifier, instance_id],
            [namespace, "403ErrorRate", "Region", "Global", identifier, instance_id],
            [namespace, "404ErrorRate", "Region", "Global", identifier, instance_id]
        ],
        "5xxErrorRate": [
            [namespace, "5xxErrorRate", "Region", "Global", identifier, instance_id, {"label": "Total5xxErrors"}],
            [namespace, "502ErrorRate", "Region", "Global", identifier, instance_id],
            [namespace, "503ErrorRate", "Region", "Global", identifier, instance_id],
            [namespace, "504ErrorRate", "Region", "Global", identifier, instance_id]
        ],
        "OriginLatency": [[namespace, "OriginLatency", "Region", "Global", identifier, instance_id]],
        "CacheHitRate": [[namespace, "CacheHitRate", "Region", "Global", identifier, instance_id]]
    }

    metrics = base_metrics.get(metric_name, [[namespace, metric_name, identifier, instance_id]])

    return {
        "height": 5,
        "width": 5,
        "y": 90,
        "x": 0,
        "type": "metric",
        "properties": {
            **common_props,
            "metrics": metrics
        }
    }




METRICS = [
    "Requests",
    "DataTransfer",
    "TotalErrorRate",
    "4xxErrorRate",
    "5xxErrorRate",
    "OriginLatency",
    "CacheHitRate"
]

# Replace with actual distribution IDs per microservice domain if needed
MICRO_SERVICE_MAP = {
    "mamaearth": {
        "catalog.mamaearth.in": "E1Z25GBCTJ805Q",
        "cart.mamaearth.in": "EVIQ7AQNYX0D",
        "customer.mamaearth.in": "E8IYPYGRZ3HTC",
        "auth.mamaearth.in": "E1LNIDW5YEXSUQ",
        "external.mamaearth.in": "E3MRD0TVB9SAIH",
        "order.mamaearth.in": "E5JRXEQRBPM0Q", 
        "mamaearth.in-PWA": "E39S17X1VRCE3P", 
        "mmrth-mg-api-v2.honasa-production.net": "E3HJ74ZZ41LAVA",
        "mmrth-mg-cs-v2.honasa-production.net": "E21NEX2LV14K99"
    }, 
    "tdc": {
        "catalog.thedermaco.com": "E2RTIXS4S07EDG",
        "cart.thedermaco.com": "E2CGIYVMN0QOGH",
        "customer.thedermaco.in": "E1ONWA4GW90RJR",
        "auth.thedermaco.in": "EXUQHGFP74NIS",
        "external.thedermaco.in": "E334BDCUS8PEI4",
        "order.thedermaco.in": "E1KT64ZZM7S6W8", 
        "thedermaco.com-PWA": "E2CJYYY6RG9DQA", 
        "tdc-mg-cs-v1.honasa-production.net": "E1ZIV36YU3LTJP", 
        "tdc-mg-api-v1.honasa-production.net": "EKSYH3TMMS3A7"
    }, 

}

def main():

    dashboard_name = "honasa-mamaearth-cloudfront" 
    domain = "mamaearth"

    for micro_service, dist_id in MICRO_SERVICE_MAP[domain].items():
        for metric in METRICS:
            add_metric_widget(dashboard_name, "AWS/CloudFront", dist_id, metric, micro_service)



if __name__ == "__main__":
    main()

