from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import boto3
import pandas as pd
import io
import math
import os
import argparse

from dataset.keys import list_keys_from_inventory
from dataset.utils import parallel_map, prune_fields, serial_map
from dataset.manifest import build_html_manifest, build_json_manifest
from dataset.event_registry import get_event_config
from dataset.datashop import handle_datashop, download_data_shop_context, post_process_datashop_context

def generate_datashop(context):
    
    # Initialize the Spark context and S3 client
    sc, spark = initialize_spark_context("generate_dataset")
    s3_client = boto3.client('s3')

    # Define key parameters
    source_bucket = context["bucket_name"]
    inventory_bucket = context["inventory_bucket_name"]
    target_prefix = f'{context["job_id"]}/'
    chunk_size = context["chunk_size"]
    section_ids = context["section_ids"]

    # Retrieve matching keys from S3 inventory
    keys = list_keys_from_inventory(section_ids, "attempt_evaluated", source_bucket, inventory_bucket)
    number_of_chunks = calculate_number_of_chunks(len(keys), chunk_size)

    print(f"Context: {context}")
    print(f"Number of keys: {len(keys)}")
    print(f"Number of chunks: {number_of_chunks}")

    # Retrieve the datashop job context
    datashop_context = download_data_shop_context(s3_client, context)
    post_process_datashop_context(datashop_context)

    context['datashop_context'] = datashop_context

    # Every XML chunk should have the <?xml ?> directive and the
    # outermost tutor_related_message_sequence element
    chunk_prefix =  """<?xml version= \"1.0\" encoding= \"UTF-8\"?>
    <tutor_related_message_sequence version_number= \"4\" xmlns:xsi= \"http://www.w3.org/2001/XMLSchema-instance\" xsi:noNamespaceSchemaLocation= \"http://pslcdatashop.org/dtd/tutor_message_v4.xsd\">
    """
    chunk_suffix = "</tutor_related_message_sequence>"

    # Process keys in chunks, serially
    for chunk_index, chunk_keys in enumerate(chunkify(keys, chunk_size)):
        try:
            # Process keys in parallel to 
            chunk_data = parallel_map(sc, source_bucket, chunk_keys, handle_datashop, context, [])
            #chunk_data = serial_map(source_bucket, chunk_keys, handle_datashop, context, [])

            chunk_data = [chunk_prefix] + chunk_data + [chunk_suffix]
            
            # Save the collected results as an XML chunk to S3
            save_xml_chunk(chunk_data, s3_client, target_prefix, chunk_index)
            print(f"Successfully processed chunk {chunk_index + 1}/{number_of_chunks}")

        except Exception as e:
            print(f"Error processing chunk {chunk_index + 1}/{number_of_chunks}: {e}")

    # Build and save JSON and HTML manifests
    context['datashop_context'] = {}
    build_manifests(s3_client, context, number_of_chunks, "xml")

    # Stop Spark context
    sc.stop()

    return number_of_chunks


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

    # Create a list of indices of field to remove, to honor the exclude_fields parameter
    column_indices_map = {column: index for index, column in enumerate(columns)}
    excluded_indices = [column_indices_map[column] for column in context["exclude_fields"]]
    excluded_indices.sort(reverse=True)
    columns = prune_fields(columns, excluded_indices)

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
            chunk_data = parallel_map(sc, source_bucket, chunk_keys, event_jsonl_processor, context, excluded_indices)
            
            # Save the collected results as a CSV file to S3
            save_chunk_to_s3(chunk_data, columns, s3_client, target_prefix, chunk_index)
            print(f"Successfully processed chunk {chunk_index + 1}/{number_of_chunks}")

        except Exception as e:
            print(f"Error processing chunk {chunk_index + 1}/{number_of_chunks}: {e}")

    # Build and save JSON and HTML manifests
    build_manifests(s3_client, context, number_of_chunks, "csv")

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

def save_xml_chunk(chunk_data, s3_client, target_prefix, chunk_index):
    
    # concatenate the strings in the list
    xml_string = '\n'.join(chunk_data)

    chunk_key = f'{target_prefix}chunk_{chunk_index}.xml'
    s3_client.put_object(Bucket="torus-datasets-prod", Key=chunk_key, Body=xml_string)


def build_manifests(s3_client, context, number_of_chunks, extension):
    """Build HTML and JSON manifests."""
    build_html_manifest(s3_client, context, number_of_chunks, extension)
    build_json_manifest(s3_client, context, number_of_chunks, extension)

   

