#!/bin/bash

export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_DEFAULT_REGION=us-east-1

BUCKET="rearc-quest"
ENDPOINT="http://localhost:9000"

if aws s3api head-bucket \
    --bucket "$BUCKET" \
    --endpoint-url "$ENDPOINT" 2>/dev/null; then

    echo "Bucket $BUCKET already exists"

else

    echo "Bucket $BUCKET does not exist. Creating..."

    aws s3api create-bucket \
        --bucket "$BUCKET" \
        --endpoint-url "$ENDPOINT" \
        --region us-east-1

    echo "Bucket created"

fi
 
echo "Starting the local run"
curl -XPOST "http://localhost:9002/2015-03-31/functions/function/invocations" -d '{}'
echo "Invoking lambda step 2"
curl -XPOST "http://localhost:9003/2015-03-31/functions/function/invocations" -d '{}'
echo "Invoking lambda step 3"
curl -XPOST "http://localhost:9004/2015-03-31/functions/function/invocations" -d '{}'