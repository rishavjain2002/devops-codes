import boto3
from datetime import datetime, timedelta
from credentials import REGION, aws_access_key_id, aws_secret_access_key



def get_aws_console():
    aws_console = boto3.Session(
        aws_access_key_id= aws_access_key_id,
        aws_secret_access_key= aws_secret_access_key
    )
    return aws_console

def get_ec2_console():
    aws_console = get_aws_console()
    cf_console = aws_console.client(service_name = "ec2", region_name = REGION)
    return cf_console


# ------------------------------------------------------------------------------------------------------------- 


def createAMI(instance_ids):
    DESCRIPTION = f"Automated weekly backup on {datetime.now().strftime('%Y%m%d')}"
    print(DESCRIPTION)
    ec2_console = get_ec2_console()
    for instance_id in instance_ids:
        ami_name = f"backup-{instance_id}-{datetime.now().strftime('%Y%m%d')}"
        print(f"Creating AMI: {ami_name} for instance: {instance_id}")
        
        response = ec2_console.create_image(
            InstanceId=instance_id,
            Name=ami_name,
            Description=DESCRIPTION,
            NoReboot=True
        )
        ami_id = response['ImageId']
        print(f"AMI created: {ami_id}")

        ec2_console.create_tags(
            Resources=[ami_id],
            Tags=[{'Key': 'Backup', 'Value': 'mondayScript'}]
        )


def deregisterAMI():
    RETENTION_DAYS = 1
    ec2_console = get_ec2_console()
    cutoff_date = (datetime.now() - timedelta(days=RETENTION_DAYS)).strftime('%Y-%m-%dT%H:%M:%S')
    print(f"Cutoff date: {cutoff_date}")

    images = ec2_console.describe_images(
        Owners=['self'],
        Filters=[{'Name': 'tag:Backup', 'Values': ['SundayScript']}]
    )['Images']

    print(images)
    old_amis = []
    for img in images:
        if img['CreationDate'] < cutoff_date:
            old_amis.append(img['ImageId'])

    for ami_id in old_amis:
        print(f"Deregistering AMI {ami_id} ...")
        ec2_console.deregister_image(ImageId=ami_id)

