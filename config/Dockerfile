# Use the base image for EMR Serverless with Spark
FROM --platform=linux/amd64 public.ecr.aws/emr-serverless/spark/emr-6.12.0:latest AS base

# Switch to root user for installing dependencies
USER root

# Install Python dependencies (excluding argparse and ssl)
RUN python3 -m pip install -U pip

RUN python3 -m pip install boto3 
RUN python3 -m pip install botocore 
RUN python3 -m pip install s3transfer 
RUN python3 -m pip install urllib3 
RUN python3 -m pip install pandas 
RUN python3 -m pip install datetime
RUN python3 -m pip install pyarrow fastparquet


# Set EMR Serverless user permissions
USER hadoop:hadoop
