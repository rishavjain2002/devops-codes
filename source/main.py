import boto3
from helper import createAMI, deregisterAMI
instance_ids = ["i-02391bc6e8b42b240"] 

def main():
    # createAMI(instance_ids)
    deregisterAMI()

main()