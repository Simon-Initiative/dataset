#!/bin/bash

# Check if two arguments are provided
if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <ACTION> <EVENT_SUB_TYPES> <SECTION_IDS>"
  exit 1
fi

# Assign command-line arguments to variables
ACTION="$1"
SUB_TYPES="$2"
SECTION_IDS="$3"

# Define the application name you want to search for
TARGET_APP_NAME="csv_job"

# List all EMR Serverless applications and find the ID of the target application
APP_ID=$(aws emr-serverless list-applications --query "applications[?name=='${TARGET_APP_NAME}'].id" --output text)

# Check if the application ID was found
if [ -z "$APP_ID" ]; then
  echo "Error: Application with name '${TARGET_APP_NAME}' not found."
  exit 1
fi

echo "Found Application ID: $APP_ID"

# Define the parameters for the job
ENTRY_POINT="s3://analyticsjobs/job.py"  # Update with your script path
ROLE_ARN="arn:aws:iam::762438811603:role/service-role/AmazonEMR-ExecutionRole-1731366715097"  # Update with your IAM role
LOG_URI="s3://analyticsjobs/logs/"  # Update with your log bucket
SPARK_PARAMS="--conf spark.executor.memory=2G --conf spark.executor.cores=2"  # Customize as needed

JOB_ID="$(date '+%Y%m%d%H%M%S')-$(LC_ALL=C tr -dc 'a-zA-Z0-9' < /dev/urandom | head -c 5)"

# Construct the JSON blob
cat <<EOT > job-config.json
{
  "applicationId": "$APP_ID",
  "executionRoleArn": "$ROLE_ARN",
  "jobDriver": {
    "sparkSubmit": {
      "entryPoint": "$ENTRY_POINT",
      "entryPointArguments": [
        "--bucket_name",
        "torus-xapi-prod",
        "--chunk_size",
        "10000",
        "--ignored_student_ids",
        "1,2",
        "--sub_types",
        "$SUB_TYPES",
        "--job_id",
        "$JOB_ID",
        "--section_ids",
        "$SECTION_IDS",
        "--action",
        "$ACTION"
      ],
      "sparkSubmitParameters": "--conf spark.archives=s3://analyticsjobs/dataset.zip#dataset --py-files s3://analyticsjobs/dataset.zip $SPARK_PARAMS"
    }
  },
  "configurationOverrides": {
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": {
        "logUri": "$LOG_URI"
      }
    }
  }
}
EOT

echo "Job configuration JSON created: job-config.json"

# Submit the job (optional step, uncomment if you want to submit the job immediately)
aws emr-serverless start-job-run --cli-input-json file://job-config.json
