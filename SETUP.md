# Torus Setup Instructions

## Create AWS S3 Buckets

Create a bucket to store the created datasets.  

Deploy `job.py` and `dataset.zip` into locations into locations in S3.  The `deploy_prod.sh` and `deploy_test.sh` show how to do this for OLI specific infrastructure.

## Build and Publish Image for Dependencies

A custom image must be created and published to the Amazon Elastic Container Registry. 
Build the docker image to capture all necessary EMR dependencies using `config/Dockerfile` via `config/update_image.sh`.  Note: `config/update.sh` contains OLI specific details, customize this is deploying to a different environment.
 

## Create EMR Serverless Application

Using EMR Serverless studio, create an EMR application of type "Spark" using `emr-6.12.0` and `x86_64` architecture. 

For custom image settings, specify the image URI from the Elastic Container Registry to use
for both Spark drivers and executors.  

## Set Torus Environment Variables

```
EMR_DATASET_EXECUTION_ROLE=<your EMR execution role>
EMR_DATASET_ENABLED=true
EMR_DATASET_APPLICATION_NAME=<the name of your EMR serverless application>
EMR_DATASET_ENTRY_POINT=<location of entry point, e.g. s3://analyticsjobs/job.py>
EMR_DATASET_LOG_URI=<location of where to collect logs, e.g. s3://analyticsjobs/logs>
EMR_DATASET_SOURCE_BUCKET=<source xapi bucket name, e.g. torus-xapi-prod>
EMR_DATASET_CONTEXT_BUCKET=<bucket to upload context to, e.g. torus-datasets-prod>
EMR_DATASET_SPARK_SUBMIT_PARAMETERS=<spark params, including location of dataset.zip, see below>
```

An example of `EMR_DATASET_SPARK_SUBMIT_PARAMETERS`:

```
--conf spark.archives=s3://analyticsjobs/dataset.zip#dataset --py-files s3://analyticsjobs/dataset.zip --conf spark.executor.memory=2G --conf spark.executor.cores=2
```