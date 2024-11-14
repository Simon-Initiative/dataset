from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import boto3
import pandas as pd
import io
import math
import os
import argparse

from dataset.keys import list_keys_from_inventory
from dataset.utils import parallel_map
from dataset.manifest import build_html_manifest, build_json_manifest
from dataset.event_registry import get_event_config


def generate_dataset(section_ids, action, context):
    """function to generate the dataset for given section IDs and action."""

    # Initialize the Spark context and S3 client
    sc, spark = initialize_spark_context("generate_dataset")
    s3_client = boto3.client('s3')

    # Define key parameters
    source_bucket = context["bucket_name"]
    inventory_bucket = context["inventory_bucket_name"]
    target_prefix = f'{context["job_id"]}/'
    chunk_size = context["chunk_size"]
    event_jsonl_processor, columns = get_event_config(action)

    # Retrieve matching keys from S3 inventory
    keys = list_keys_from_inventory(section_ids, action, source_bucket, inventory_bucket)
    number_of_chunks = calculate_number_of_chunks(len(keys), chunk_size)

    print(f"Context: {context}")
    print(f"Number of keys: {len(keys)}")
    print(f"Number of chunks: {number_of_chunks}")

    # Process keys in chunks, serially
    for chunk_index, chunk_keys in enumerate(chunkify(keys, chunk_size)):
        try:
            # Process keys in parallel
            chunk_data = parallel_map(sc, source_bucket, chunk_keys, event_jsonl_processor, context)
            
            # Save the collected results as a CSV file to S3
            save_chunk_to_s3(chunk_data, columns, s3_client, target_prefix, chunk_index)
            print(f"Successfully processed chunk {chunk_index + 1}/{number_of_chunks}")

        except Exception as e:
            print(f"Error processing chunk {chunk_index + 1}/{number_of_chunks}: {e}")

    # Build and save JSON and HTML manifests
    build_manifests(s3_client, context["bucket_name"], context["job_id"], number_of_chunks)

    # Stop Spark context
    sc.stop()

    return number_of_chunks


def initialize_spark_context(app_name):
    """Initialize and return a Spark context and session."""
    conf = SparkConf().setAppName(app_name)
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    return sc, spark


def calculate_number_of_chunks(total_keys, chunk_size):
    """Calculate the number of chunks based on total keys and chunk size."""
    return math.ceil(total_keys / chunk_size)


def chunkify(lst, chunk_size):
    """Yield successive chunks from a list."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def save_chunk_to_s3(chunk_data, columns, s3_client, target_prefix, chunk_index):
    """Save a DataFrame as a CSV file to S3."""

    df = pd.DataFrame(chunk_data, columns=columns)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    chunk_key = f'{target_prefix}chunk_{chunk_index}.csv'
    s3_client.put_object(Bucket="torus-datasets-prod", Key=chunk_key, Body=csv_buffer.getvalue())


def build_manifests(s3_client, bucket_name, job_id, number_of_chunks):
    """Build HTML and JSON manifests."""
    build_html_manifest(s3_client, bucket_name, job_id, number_of_chunks)
    build_json_manifest(s3_client, bucket_name, job_id, number_of_chunks)

   

