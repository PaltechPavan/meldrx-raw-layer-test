#!/usr/bin/env bash
# MeldRx raw layer - test account setup (DATA-200). Run section by section in one bash session;
# the PDF guide explains each step and the expected results. Not meant to be run unattended.

# ===== Step 1 - Set variables =====
export AWS_REGION=us-east-1           # any region; us-east-1 matches our setup
export AWS_DEFAULT_REGION=$AWS_REGION
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

SRC_BUCKET=meldrx-test-source-$ACCOUNT_ID    # stands in for the app team's delivery bucket
RAW_BUCKET=meldrx-test-raw-$ACCOUNT_ID       # our raw layer
ATHENA_BUCKET=meldrx-test-athena-$ACCOUNT_ID # Athena query results
RAW_DB=meldrx_test_raw_db
WG=meldrx-test-wg
REPO=meldrx-test-ingest
CLUSTER=meldrx-test-cluster
FAMILY=meldrx-test-task
LOG_GROUP=/ecs/meldrx-test
ALERT_EMAIL=you@example.com                  # <-- change: receives failure alerts
mkdir -p meldrx-test && cd meldrx-test

# ===== Step 2 - KMS keys =====
SRC_KEY_ARN=$(aws kms create-key --description "meldrx test - source" --query KeyMetadata.Arn --output text)
aws kms create-alias --alias-name alias/meldrx-test-source --target-key-id "$SRC_KEY_ARN"
RAW_KEY_ARN=$(aws kms create-key --description "meldrx test - raw layer" --query KeyMetadata.Arn --output text)
aws kms create-alias --alias-name alias/meldrx-test-raw --target-key-id "$RAW_KEY_ARN"
echo "$SRC_KEY_ARN"; echo "$RAW_KEY_ARN"

# ===== Step 3 - S3 buckets =====
for B in $SRC_BUCKET $RAW_BUCKET $ATHENA_BUCKET; do
  aws s3api create-bucket --bucket $B
  aws s3api put-public-access-block --bucket $B --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
done
enc() { printf '{"Rules":[{"ApplyServerSideEncryptionByDefault":%s,"BucketKeyEnabled":true}]}' \
          "{\"SSEAlgorithm\":\"aws:kms\",\"KMSMasterKeyID\":\"$1\"}"; }
aws s3api put-bucket-encryption --bucket $SRC_BUCKET --server-side-encryption-configuration "$(enc $SRC_KEY_ARN)"
aws s3api put-bucket-encryption --bucket $RAW_BUCKET --server-side-encryption-configuration "$(enc $RAW_KEY_ARN)"
aws s3api put-bucket-versioning --bucket $SRC_BUCKET --versioning-configuration Status=Enabled

cat > raw-lifecycle.json <<EOF
{"Rules":[{"ID":"LandingToGlacierIR","Status":"Enabled","Filter":{"Prefix":"meldrx/landing/"},
           "Transitions":[{"Days":90,"StorageClass":"GLACIER_IR"}]}]}
EOF
aws s3api put-bucket-lifecycle-configuration --bucket $RAW_BUCKET --lifecycle-configuration file://raw-lifecycle.json
cat > athena-lifecycle.json <<EOF
{"Rules":[{"ID":"Expire7d","Status":"Enabled","Filter":{"Prefix":""},"Expiration":{"Days":7}}]}
EOF
aws s3api put-bucket-lifecycle-configuration --bucket $ATHENA_BUCKET --lifecycle-configuration file://athena-lifecycle.json

# ===== Step 4 - Glue database and Athena workgroup =====
aws glue create-database --database-input Name=$RAW_DB
WG_CONF="ResultConfiguration={OutputLocation=s3://$ATHENA_BUCKET/results/}"
WG_CONF="$WG_CONF,EnforceWorkGroupConfiguration=true,PublishCloudWatchMetricsEnabled=true"
aws athena create-work-group --name $WG --configuration "$WG_CONF"

# ===== Step 5a - IAM: task execution role =====
cat > trust-ecs-tasks.json <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ecs-tasks.amazonaws.com"},
  "Action":"sts:AssumeRole","Condition":{"StringEquals":{"aws:SourceAccount":"$ACCOUNT_ID"}}}]}
EOF
aws iam create-role --role-name meldrx-test-ecs-execution-role --assume-role-policy-document file://trust-ecs-tasks.json
aws iam attach-role-policy --role-name meldrx-test-ecs-execution-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy
EXEC_ROLE_ARN=arn:aws:iam::$ACCOUNT_ID:role/meldrx-test-ecs-execution-role

# ===== Step 5b - IAM: task role (what the ingest and the downstream step may do) =====
cat > task-role-policy.json <<EOF
{"Version":"2012-10-17","Statement":[
 {"Sid":"ListSourceVersions","Effect":"Allow","Action":["s3:ListBucket","s3:ListBucketVersions"],
  "Resource":"arn:aws:s3:::$SRC_BUCKET","Condition":{"StringLike":{"s3:prefix":["patient_access/*"]}}},
 {"Sid":"ReadSourceVersions","Effect":"Allow","Action":["s3:GetObject","s3:GetObjectVersion"],
  "Resource":"arn:aws:s3:::$SRC_BUCKET/patient_access/*"},
 {"Sid":"DecryptSource","Effect":"Allow","Action":"kms:Decrypt","Resource":"$SRC_KEY_ARN"},
 {"Sid":"ListRaw","Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::$RAW_BUCKET",
  "Condition":{"StringLike":{"s3:prefix":["meldrx/*"]}}},
 {"Sid":"ReadWriteRaw","Effect":"Allow","Action":["s3:GetObject","s3:PutObject"],
  "Resource":"arn:aws:s3:::$RAW_BUCKET/meldrx/*"},
 {"Sid":"BucketLocation","Effect":"Allow","Action":"s3:GetBucketLocation",
  "Resource":["arn:aws:s3:::$RAW_BUCKET","arn:aws:s3:::$ATHENA_BUCKET"]},
 {"Sid":"EncryptRaw","Effect":"Allow","Action":["kms:GenerateDataKey","kms:Decrypt"],"Resource":"$RAW_KEY_ARN"},
 {"Sid":"GlueRead","Effect":"Allow",
  "Action":["glue:GetDatabase","glue:GetTable","glue:GetTables","glue:GetPartition","glue:GetPartitions"],
  "Resource":["arn:aws:glue:$AWS_REGION:$ACCOUNT_ID:catalog","arn:aws:glue:$AWS_REGION:$ACCOUNT_ID:database/$RAW_DB",
              "arn:aws:glue:$AWS_REGION:$ACCOUNT_ID:table/$RAW_DB/*"]},
 {"Sid":"GlueWriteMeldrxTables","Effect":"Allow",
  "Action":["glue:CreateTable","glue:UpdateTable","glue:CreatePartition","glue:BatchCreatePartition"],
  "Resource":["arn:aws:glue:$AWS_REGION:$ACCOUNT_ID:catalog","arn:aws:glue:$AWS_REGION:$ACCOUNT_ID:database/$RAW_DB",
              "arn:aws:glue:$AWS_REGION:$ACCOUNT_ID:table/$RAW_DB/meldrx_raw_*"]},
 {"Sid":"AthenaPostCheck","Effect":"Allow",
  "Action":["athena:StartQueryExecution","athena:GetQueryExecution","athena:GetQueryResults","athena:GetWorkGroup"],
  "Resource":"arn:aws:athena:$AWS_REGION:$ACCOUNT_ID:workgroup/$WG"},
 {"Sid":"AthenaResults","Effect":"Allow","Action":["s3:GetObject","s3:PutObject","s3:ListBucket"],
  "Resource":["arn:aws:s3:::$ATHENA_BUCKET","arn:aws:s3:::$ATHENA_BUCKET/*"]}
]}
EOF
aws iam create-role --role-name meldrx-test-task-role --assume-role-policy-document file://trust-ecs-tasks.json
aws iam put-role-policy --role-name meldrx-test-task-role --policy-name meldrx-ingest \
  --policy-document file://task-role-policy.json
TASK_ROLE_ARN=arn:aws:iam::$ACCOUNT_ID:role/meldrx-test-task-role

# ===== Step 5c - IAM: scheduler role =====
cat > trust-scheduler.json <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},
  "Action":"sts:AssumeRole","Condition":{"StringEquals":{"aws:SourceAccount":"$ACCOUNT_ID"}}}]}
EOF
cat > scheduler-policy.json <<EOF
{"Version":"2012-10-17","Statement":[
 {"Effect":"Allow","Action":"ecs:RunTask",
  "Resource":["arn:aws:ecs:$AWS_REGION:$ACCOUNT_ID:task-definition/$FAMILY",
              "arn:aws:ecs:$AWS_REGION:$ACCOUNT_ID:task-definition/$FAMILY:*"],
  "Condition":{"ArnEquals":{"ecs:cluster":"arn:aws:ecs:$AWS_REGION:$ACCOUNT_ID:cluster/$CLUSTER"}}},
 {"Effect":"Allow","Action":"iam:PassRole","Resource":["$EXEC_ROLE_ARN","$TASK_ROLE_ARN"],
  "Condition":{"StringLike":{"iam:PassedToService":"ecs-tasks.amazonaws.com"}}}
]}
EOF
aws iam create-role --role-name meldrx-test-scheduler-role --assume-role-policy-document file://trust-scheduler.json
aws iam put-role-policy --role-name meldrx-test-scheduler-role --policy-name run-meldrx-task \
  --policy-document file://scheduler-policy.json
SCHED_ROLE_ARN=arn:aws:iam::$ACCOUNT_ID:role/meldrx-test-scheduler-role

# ===== Step 6 - ECR repository and container image =====
aws ecr create-repository --repository-name $REPO --image-tag-mutability IMMUTABLE \
  --image-scanning-configuration scanOnPush=true
IMAGE=$ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$REPO:v1
aws ecr get-login-password | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com
docker build --platform linux/amd64 -t $IMAGE ../meldrx-raw-layer-test-kit/app
docker push $IMAGE

# ===== Step 7 - Network =====
VPC_ID=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
SUBNETS=$(aws ec2 describe-subnets --filters Name=vpc-id,Values=$VPC_ID Name=default-for-az,Values=true \
  --query 'Subnets[0:2].SubnetId' --output text | tr '\t' ',')
SG_ID=$(aws ec2 create-security-group --group-name meldrx-test-task-sg --description "meldrx test task, egress only" \
  --vpc-id $VPC_ID --query GroupId --output text)
echo "$VPC_ID $SUBNETS $SG_ID"      # no default VPC? create one: aws ec2 create-default-vpc

# ===== Step 8 - ECS cluster, log group and task definition =====
MSYS_NO_PATHCONV=1 aws logs create-log-group --log-group-name $LOG_GROUP
MSYS_NO_PATHCONV=1 aws logs put-retention-policy --log-group-name $LOG_GROUP --retention-in-days 30
aws ecs create-cluster --cluster-name $CLUSTER --capacity-providers FARGATE \
  --default-capacity-provider-strategy capacityProvider=FARGATE,weight=1
cat > taskdef.json <<EOF
{"family":"$FAMILY","requiresCompatibilities":["FARGATE"],"networkMode":"awsvpc","cpu":"256","memory":"512",
 "executionRoleArn":"$EXEC_ROLE_ARN","taskRoleArn":"$TASK_ROLE_ARN",
 "containerDefinitions":[{"name":"app","image":"$IMAGE","essential":true,
   "environment":[
     {"name":"MELDRX_SOURCE_URI","value":"s3://$SRC_BUCKET/patient_access/"},
     {"name":"MELDRX_RAW_URI","value":"s3://$RAW_BUCKET/meldrx/"},
     {"name":"MELDRX_RAW_DB","value":"$RAW_DB"},
     {"name":"MELDRX_KMS_KEY","value":"$RAW_KEY_ARN"},
     {"name":"MELDRX_INGEST_ENABLED","value":"true"},
     {"name":"ATHENA_WORKGROUP","value":"$WG"},
     {"name":"AWS_DEFAULT_REGION","value":"$AWS_REGION"}],
   "logConfiguration":{"logDriver":"awslogs","options":{"awslogs-group":"$LOG_GROUP",
     "awslogs-region":"$AWS_REGION","awslogs-stream-prefix":"app"}}}]}
EOF
MSYS_NO_PATHCONV=1 aws ecs register-task-definition --cli-input-json file://taskdef.json --query taskDefinition.revision

# ===== Step 9 - Failure alert (SNS email) =====
TOPIC_ARN=$(aws sns create-topic --name meldrx-test-pipeline-alerts --query TopicArn --output text)
aws sns subscribe --topic-arn $TOPIC_ARN --protocol email --notification-endpoint $ALERT_EMAIL
CLUSTER_ARN=arn:aws:ecs:$AWS_REGION:$ACCOUNT_ID:cluster/$CLUSTER
cat > rule-pattern.json <<EOF
{"source":["aws.ecs"],"detail-type":["ECS Task State Change"],
 "detail":{"clusterArn":["$CLUSTER_ARN"],"lastStatus":["STOPPED"],
   "\$or":[{"containers":{"exitCode":[{"anything-but":0}]}},{"stopCode":["TaskFailedToStart"]}]}}
EOF
RULE_ARN=$(aws events put-rule --name meldrx-test-task-failed --event-pattern file://rule-pattern.json \
  --query RuleArn --output text)
cat > topic-policy.json <<EOF
{"Version":"2012-10-17","Statement":[{"Sid":"EventBridgePublish","Effect":"Allow",
  "Principal":{"Service":"events.amazonaws.com"},"Action":"sns:Publish","Resource":"$TOPIC_ARN",
  "Condition":{"ArnEquals":{"aws:SourceArn":"$RULE_ARN"}}}]}
EOF
aws sns set-topic-attributes --topic-arn $TOPIC_ARN --attribute-name Policy --attribute-value file://topic-policy.json
aws events put-targets --rule meldrx-test-task-failed --targets "Id=sns,Arn=$TOPIC_ARN"

# ===== Step 10 - Helper to run the job and wait for it =====
run_job() {
  local extra=(); [ -n "$1" ] && extra=(--overrides "$1")
  TASK_ARN=$(aws ecs run-task --cluster $CLUSTER --task-definition $FAMILY --launch-type FARGATE \
    --network-configuration "awsvpcConfiguration={subnets=[$SUBNETS],securityGroups=[$SG_ID],assignPublicIp=ENABLED}" \
    "${extra[@]}" --query 'tasks[0].taskArn' --output text)
  echo "started $TASK_ARN"
  aws ecs wait tasks-stopped --cluster $CLUSTER --tasks $TASK_ARN
  aws ecs describe-tasks --cluster $CLUSTER --tasks $TASK_ARN \
    --query 'tasks[0].{exitCode:containers[0].exitCode,stopCode:stopCode,reason:stoppedReason}'
  MSYS_NO_PATHCONV=1 aws logs tail $LOG_GROUP --log-stream-names app/app/${TASK_ARN##*/} --format short | tail -40
}
env_override() {   # env_override NAME VALUE  -> overrides JSON for one environment variable
  echo "{\"containerOverrides\":[{\"name\":\"app\",\"environment\":[{\"name\":\"$1\",\"value\":\"$2\"}]}]}"
}

# ===== Step 11 - Load test packages and run the first job =====
python3 ../meldrx-raw-layer-test-kit/make_test_packages.py testdata
aws s3 cp testdata/round1 s3://$SRC_BUCKET/patient_access/ --recursive
run_job
# Expect: exitCode 0; log events file_loaded x3, table_created x4, ingest_finished with
# "new_versions": 3, "files_loaded": 3, then post_check lines for 4 tables with rows_ = 3 each.

# ===== Section 6.4 - Schedule test (S11) =====
SUBNETS_JSON=$(echo "$SUBNETS" | sed 's/,/","/g')
cat > schedule-target.json <<EOF
{"Arn":"arn:aws:ecs:$AWS_REGION:$ACCOUNT_ID:cluster/$CLUSTER","RoleArn":"$SCHED_ROLE_ARN",
 "EcsParameters":{"TaskDefinitionArn":"arn:aws:ecs:$AWS_REGION:$ACCOUNT_ID:task-definition/$FAMILY",
   "LaunchType":"FARGATE","TaskCount":1,
   "NetworkConfiguration":{"awsvpcConfiguration":{"Subnets":["$SUBNETS_JSON"],
     "SecurityGroups":["$SG_ID"],"AssignPublicIp":"ENABLED"}}},
 "RetryPolicy":{"MaximumRetryAttempts":0}}
EOF
# Test cadence: every 15 minutes. Production: cron(30 0 * * ? *) = daily 00:30 UTC.
aws scheduler create-schedule --name meldrx-test-every-15-min --schedule-expression "rate(15 minutes)" \
  --flexible-time-window Mode=OFF --state ENABLED --target file://schedule-target.json
# If this is rejected because the task definition ARN has no revision, retry with ...:task-definition/$FAMILY:1
# and record it in the results sheet (it changes how releases must update the schedule).

# ===== Section 9 - Clean-up =====
aws scheduler delete-schedule --name meldrx-test-every-15-min 2>/dev/null
aws events remove-targets --rule meldrx-test-task-failed --ids sns; aws events delete-rule --name meldrx-test-task-failed
aws sns delete-topic --topic-arn $TOPIC_ARN
for R in $(aws ecs list-task-definitions --family-prefix $FAMILY --query taskDefinitionArns --output text); do
  aws ecs deregister-task-definition --task-definition $R >/dev/null; done
aws ecs delete-cluster --cluster $CLUSTER
aws ecr delete-repository --repository-name $REPO --force
for B in $SRC_BUCKET $RAW_BUCKET $ATHENA_BUCKET; do
  python3 -c "import boto3,sys; boto3.resource('s3').Bucket(sys.argv[1]).object_versions.delete()" $B
  aws s3api delete-bucket --bucket $B; done
aws glue delete-database --name $RAW_DB
aws athena delete-work-group --work-group $WG --recursive-delete-option
for R in meldrx-test-task-role:meldrx-ingest meldrx-test-scheduler-role:run-meldrx-task; do
  aws iam delete-role-policy --role-name ${R%%:*} --policy-name ${R##*:}; aws iam delete-role --role-name ${R%%:*}; done
aws iam detach-role-policy --role-name meldrx-test-ecs-execution-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy
aws iam delete-role --role-name meldrx-test-ecs-execution-role
MSYS_NO_PATHCONV=1 aws logs delete-log-group --log-group-name $LOG_GROUP
aws ec2 delete-security-group --group-id $SG_ID
for K in $SRC_KEY_ARN $RAW_KEY_ARN; do aws kms schedule-key-deletion --key-id $K --pending-window-in-days 7; done
aws kms delete-alias --alias-name alias/meldrx-test-source; aws kms delete-alias --alias-name alias/meldrx-test-raw
