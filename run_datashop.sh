#!/bin/bash

# Check if two or three arguments are provided
if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
  echo "Usage: $0 <JOB_ID> <SECTION_IDS> [<IGNORED_STUDENT_IDS>]"
  exit 1
fi

# Assign command-line arguments to variables
JOB_ID="$1"
SECTION_IDS="$2"

# Assign the optional third argument if provided
if [ -n "$3" ]; then
  IGNORED_STUDENT_IDS="$3"
else
  IGNORED_STUDENT_IDS="1"
fi

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
        "1000",
        "--ignored_student_ids",
        "$IGNORED_STUDENT_IDS",
        "--job_id",
        "$JOB_ID",
        "--section_ids",
        "$SECTION_IDS",
        "--action",
        "datashop",
        "--page_ids",
        "all"
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
