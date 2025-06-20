import boto3
from credentials import region_name, aws_access_key_id, aws_secret_access_key

 
def get_aws_console():
    aws_console = boto3.Session(
        aws_access_key_id= aws_access_key_id,
        aws_secret_access_key= aws_secret_access_key
    )
    return aws_console

def get_cf_console():
    aws_console = get_aws_console()
    cf_console = aws_console.client(service_name = "cloudfront", region_name = region_name)
    return cf_console

# ------------------------------------------------------------------------------------------------------------------------------------------------------ #
def listDistributionsist():
    response = get_cf_console().list_distributions()

    if response['DistributionList']['Quantity'] > 0:
        for distribution in response['DistributionList']['Items']:
            print(f"Distribution ID: {distribution['Id']}")
            print(f"Domain Name: {distribution['DomainName']}")
            print(f"Comment: {distribution['Comment']}")
            print("-" * 50)
    else:
        print("No distributions found.")
 

def copyDistribution(primary_distribution_id):
    response = get_cf_console().get_distribution(Id=primary_distribution_id)

    etag = response['ETag']
    print(f"ETag for the distribution: {etag}")
    # distribution_config = response['Distribution']['DistributionConfig']
    # print(f"Current Staging Distribution Config: {distribution_config}")

    caller_reference = "CopyDist-20250604-1111"
    response = get_cf_console().copy_distribution(
        PrimaryDistributionId=primary_distribution_id,
        IfMatch=etag,
        Staging=True,
        CallerReference= caller_reference,
    )
    print(response['Distribution']['Id'])
    return response['Distribution']['Id']


def addAlternatedomain(newDistId, alternate_domain_names):
    response = get_cf_console().get_distribution_config(Id=newDistId)
   
    distribution_config = response['DistributionConfig']
    distribution_config['Aliases'] = {
        'Quantity': len(alternate_domain_names),
        'Items': alternate_domain_names
    }

    staging_etag = response['ETag']
    response = get_cf_console().update_distribution(
        Id= newDistId,
        IfMatch=staging_etag, 
        DistributionConfig= distribution_config
    )


def main():
    primary_distribution_id = "ETQM52ZK5JURL"

    listDistributionsist()
    newDistId = copyDistribution(primary_distribution_id)
    newDistId = "E1RK2YJC20U8EJ"
    alternate_domain_names = ['staging.example.com', 'test.example.com']
    addAlternatedomain(newDistId, alternate_domain_names)

main()