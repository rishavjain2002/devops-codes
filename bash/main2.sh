!/bin/bash

REGION="ap-south-1"
DESCRIPTION="Automated weekly backup on $(date +%Y%m%d)"
RETENTION_DAYS=14
PROFILE="ris"

instance_ids= []
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
    --profile=$PATH
    )

    echo "AMI ($AMI_ID) created"

    aws ec2 create-tags \
    --resources "$AMI_ID" \
    --tags Key=Backup,Value=SundayScript \
    --region "$REGION" \
    --profile=ris
done




CUTOFF_DATE=$(date -v -${RETENTION_DAYS}d +"%Y-%m-%dT%H:%M:%S")
echo "Cutoff date: $CUTOFF_DATE"

OLD_AMIS=$(aws ec2 describe-images \
    --owners self \
    --filters "Name=tag:Backup,Values=SundayScript" \
    --region "$REGION" \
    --query "Images[?CreationDate<'$CUTOFF_DATE'].ImageId" \
    --output text \
    --profile "$PROFILE")

# Deregister old AMIs
for AMI in $OLD_AMIS; do
    echo "Deregistering AMI $AMI ..."
    aws ec2 deregister-image --image-id "$AMI" --region "$REGION" --profile "$PROFILE"
done
