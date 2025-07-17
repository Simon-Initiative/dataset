# Custom Dataset Creation

This repository contains a parameterized PySpark job for Torus custom dataset creation, 
along with scripts to deploy, run, and manage dependencies and configurations 
for executing within AWS EMR Serverless.

---

## Table of Contents
1. [Deployment](#deployment)
2. [Running the PySpark Job](#running-the-pyspark-job)
3. [Updating the Custom Docker Image](#updating-the-custom-docker-image)
4. [Requirements](#requirements)

---

## Deployment

The entrypoint for the PySpark job for custom dataset generation is defined in `job.py`.  Supporting
modules are found in the `dataset` directory.  To be invoked in the AWS EMR Serverless environment,
these files must be deployed and accessible from an S3 bucket. 

The `deploy.sh` script automates packaging and uploading the PySpark job script and dependencies to this S3 bucket.

### Steps to Deploy:
1. Run the `deploy.sh` script from the root directory:
   ```bash
   ./deploy.sh
   ```

## Running the PySpark Job

A job can be manually invoked from EMR Serverless Studio, but also directly from the commandline using
one of two helper bash scripts. These bash scripts are wrappers around the AWS commandline tool, which you
need to install from (https://aws.amazon.com/cli/)

### Steps to Run a CSV raw data job:
1. Run the `run_job.sh` script from the root directory with arguments for action, event subtypes, and section ids
   ```bash
   ./run_job.sh attempt_evaluated part_attempt_evaluated 2342,2343
   ```
### Steps to Run a Datashop XML job:
1. Generate the context JSON file using the `context.sql` and manually upload it to the `torus-datasets-prod` bucket in the `contexts` folder,
named the same as the job id sepecified in the next step.  
2. Run the `run_datashop.sh` script from the root directory with arguments for job id and the course section ids
   ```bash
   ./run_datasohp.sh 1922 2342,2343
   ```

For the above to work, the context file must be named `1922.json` and be present in the `contexts` folder. 


## Updating the Custom Docker Image

The dependencies needed by code executed by worker and executor nodes in PySpark are supplied via a custom EMR Docker image.  Periodically,
this image may need to be updated as we expand the feature set. The
Dockerfile is present at `config/Dockerfile` and the script 
`update_image.sh` automates the building and deployment of it. 
