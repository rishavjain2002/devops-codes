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
        "AWS/EC2": "InstanceId",
        "AWS/RDS": "DBInstanceIdentifier",
        "AWS/ElastiCache": "CacheClusterId",
        "AWS/DocDB": "DBClusterIdentifier"
    }.get(namespace)


def add_metric_widget(dashboard_name, namespace, instance_id, metric_name, domain, stat="Sum", period=300):
    cloudwatch_console = get_cloudwatch_console()
    print(f"Adding {metric_name} widget for {namespace} instance ID {instance_id} in {dashboard_name} in {domain}")
    
    dashboard_body = get_dashboard_body(cloudwatch_console, dashboard_name)
    identifier = get_identifier(namespace)
    if not identifier:
        print(f"Unsupported namespace: {namespace}")
        return
    
    widget_found = False
    for wid in dashboard_body["widgets"]:
        widgetTitle = wid["properties"].get("title", "")
        if widgetTitle == f"{metric_name}-{namespace}-{domain}":
            print(f"Checking existing widget for {namespace}")
            metrics = wid["properties"]["metrics"]
            if any(instance_id in inst for inst in metrics):
                print(f"Instance {instance_id} already present")
                return
            print(f"Adding new instance {instance_id} to {namespace}")
            metrics.append([namespace, metric_name, identifier, instance_id])
            widget_found = True
            break

    if not widget_found:
        print(f"No existing widget found for {namespace}. Creating a new widget.")
        new_widget = {
            "height": 6,
            "width": 6,
            "y": len(dashboard_body["widgets"]) * 6,
            "x": 0,
            "type": "metric",
            "properties": {
                "title": f"{metric_name}-{namespace}-{domain}",
                "metrics": [[namespace, metric_name, identifier, instance_id]],
                "view": "timeSeries",
                "region": "ap-south-1",
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


def updateWidgetMetricInstances(dashboard_name, namespace, valid_instance_ids, metric_name, domain):
    cloudwatch_console = get_cloudwatch_console()
    print(f"Rechecking widget: {metric_name}-{namespace}-{domain}")

    try:
        response = cloudwatch_console.get_dashboard(DashboardName=dashboard_name)
        dashboard_body = json.loads(response['DashboardBody'])
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFound":
            print(f"Dashboard {dashboard_name} does not exist. No need to update")
            return
        else:
            print(f"Unexpected error fetching dashboard {dashboard_name}: {e}")
            return

    if(len(dashboard_body["widgets"])==0):
        print("No need to update")
        return
    
    updated = False
    for wid in dashboard_body["widgets"]:
        widgetTitle = wid["properties"].get("title", "")
        if widgetTitle == f"{metric_name}-{namespace}-{domain}":
            old_metrics = wid["properties"]["metrics"]
            new_metrics = []

            for metric in old_metrics:
                if len(metric) >= 4:
                    instance_id = metric[3]
                    if instance_id in valid_instance_ids:
                        new_metrics.append(metric)
                    else:
                        print(f"Removing instance {instance_id} from widget.")
                else:
                    new_metrics.append(metric)

            wid["properties"]["metrics"] = new_metrics
            updated = True
            print(f"{metric_name} widget for {namespace} updated.")


    if updated:
        cloudwatch_console.put_dashboard(
            DashboardName=dashboard_name,
            DashboardBody=json.dumps(dashboard_body)
        )
        print(f"Dashboard {dashboard_name} pushed successfully.")



# ------------------------------------------------------------------------- Get Intances -----------------------------------------------
def getrunningec2(tagList, domain):
    try:
        aws_console = get_aws_console()
        ec2_console = aws_console.client(service_name="ec2", region_name=REGION)
        response = ec2_console.describe_instances(
            Filters=[
                {
                    'Name': 'instance-state-name',
                    'Values': ['running']
                },
            ],
        )

        total_count = 0
        brandValue_instances = []
        for res in response["Reservations"]:
            for inst in res["Instances"]:
                total_count += 1
                tags = inst.get("Tags", [])
                has_brand_tag = any(tag['Key'] == 'brand' for tag in tags)

                if has_brand_tag:
                    instance_id = inst["InstanceId"]
                    # if any(tag['Key'] == 'brand' and tag['Value'] in brandValue for tag in tags):
                    #     brandValue_instances.append(instance_id)
                    for tag in tags:
                        if tag['Key'] == 'brand' and tag['Value'] in tagList:
                            print(f"Instance with 'brand' tag {tag['Value']}: {instance_id}")
                            brandValue_instances.append(instance_id)

                    

        print(f"Total instances: {total_count}")
        print(f"Total instances with 'brand' tag {domain}: {len(brandValue_instances)}")

        return brandValue_instances
    except Exception as e:
        print(f"Error occurred: {e}")
        print(f"Error fetching EC2 instaces: {e}")
        return []


def getactiverds(tagList, domain):
    try:
        aws_console = get_aws_console()
        rds_console = aws_console.client(service_name="rds", region_name=REGION)
        response = rds_console.describe_db_instances()

        brandValue_instances = []
        total = 0

        for res in response["DBInstances"]:
            db_id = res["DBInstanceIdentifier"]
            total += 1

            parts = db_id.split("-")
            #  add -  {and "mongo" not in parts for omni}
            if any(brand in parts and "documentdb" not in parts and "mongo" not in parts for brand in tagList):
                print(f"{db_id} contains brand value: {domain}")
                brandValue_instances.append(db_id)

        print(f"Total RDS instances: {total}")
        print(f"Instances containing brand '{domain}': {len(brandValue_instances)}")

        return brandValue_instances

    except Exception as e:
        print(f"Error occurred: {e}")
        print(f"Error fetching RDS instaces: {e}")
        return []


def getelasticCache(tagList, domain):
    try:
        aws_console = get_aws_console()
        elasticache_console = aws_console.client(service_name="elasticache", region_name=REGION)
        response = elasticache_console.describe_cache_clusters(ShowCacheNodeInfo=False)

        brandValue_instances = []
        total = 0

        for res in response["CacheClusters"]:
            cluster_id = res["CacheClusterId"]
            total += 1

            parts = cluster_id.split("-")
            if any(brand in parts for brand in tagList):
                print(f"{cluster_id} matches one of the brand values: {domain}")
                brandValue_instances.append(cluster_id)

        print(f"Total ElastiCache clusters: {total}")
        print(f"Clusters matching any brand in {domain}: {len(brandValue_instances)}")

        return brandValue_instances

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Error fetching ElastiCache instances: {e}")
        return []


def getactivedocDB(tagList, domain):
    try:
        aws_console = get_aws_console()
        docdb_console = aws_console.client(service_name="docdb", region_name=REGION)
        response = docdb_console.describe_db_clusters()

        brandValue_instances = []
        total = 0

        for res in response["DBClusters"]:
            cluster_id = res["DBClusterIdentifier"]
            total += 1

            parts = cluster_id.split("-")
            #  add -  {and "mongo" in parts for omni}
            if any(brand in parts and ("documentdb" in parts or "mongo" in parts) for brand in tagList):
                print(f"{cluster_id} matches one of the brand values: {domain}")
                brandValue_instances.append(cluster_id)

        print(f"Total DocumentDB clusters: {total}")
        print(f"Clusters matching any brand in {domain}: {len(brandValue_instances)}")

        return brandValue_instances

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Error fetching DocumentDB instances: {e}")
        return []


# Metric configuration per AWS service
SERVICE_CONFIG = {
    "AWS/EC2": {
        "fetch_func": getrunningec2,
        "metrics": ["CPUUtilization", "NetworkIn", "NetworkOut"],
    },
    "AWS/RDS": {
        "fetch_func": getactiverds,
        "metrics": [
            "CPUUtilization",
            "RollbackSegmentHistoryListLength",
            "FreeableMemory",
            "DatabaseConnections",
            "RowLockTime",
            "InsertThroughput",
            "UpdateThroughput",
            "WriteThroughput",
        ],
    },
    "AWS/ElastiCache": {
        "fetch_func": getelasticCache,
        "metrics": [
            "CPUUtilization",
            "FreeableMemory",
            "NetworkBytesIn",
            "NetworkBytesOut",
            "CurrConnections",
            "CacheHitRate",
        ],
    },
    "AWS/DocDB": {
        "fetch_func": getactivedocDB,
        "metrics": ["CPUUtilization", "FreeableMemory", "DatabaseConnections", "NetworkThroughput"],
    },
}

def process_service(dashboard_name, namespace, tagList, domain):
    fetch_func = SERVICE_CONFIG[namespace]["fetch_func"]
    metrics = SERVICE_CONFIG[namespace]["metrics"]

    instance_ids = fetch_func(tagList, domain)
    print(f"{namespace}: {instance_ids}")

    for metric in metrics:
        updateWidgetMetricInstances(dashboard_name, namespace, instance_ids, metric, domain)

    for instance_id in instance_ids:
        for metric in metrics:
            add_metric_widget(dashboard_name, namespace, instance_id, metric, domain)


def main():

    tagList, domain = ["omni", "d2c", "metabase", "yotobox"], "omni"
    dashboard_name = "honasa-omni-d2c"

    for namespace in SERVICE_CONFIG:
        process_service(dashboard_name, namespace, tagList, domain)


if __name__ == "__main__":
    main()
