#!/bin/bash

REGION="ap-south-1"
DESCRIPTION="Automated weekly backup on $(date +%Y%m%d)"

instance_ids=$(aws ec2 describe-instances \
  --filters Name=instance-state-name,Values=running \
  --query 'Reservations[*].Instances[*].InstanceId' --output text \
  --region=$REGION \
  --profile=ris)

for ID in $instance_ids; do
  AMI_NAME="backup-${ID}-$(date +%Y%m%d)"
  echo $AMI_NAME
  echo "creating AMI for ($ID)"
  AMI_ID=$(aws ec2 create-image \
    --instance-id $ID \
    --name "$AMI_NAME" \
    --description "$DESCRIPTION" \
    --no-reboot \
    --region "$REGION" \
    --output text \
    --query 'ImageId' \
    --profile=ris
    )

  echo "AMI ($AMI_ID) created"

  aws ec2 create-tags \
    --resources "$AMI_ID" \
    --tags Key=Backup,Value=SundayScript \
    --region "$REGION" \
    --profile=ris

done
