import boto3 
import pandas as pd
import io
import datetime
import re
import json

# This lists matching keys, driven from an S3 inventory bucket. The data in this bucket
# is generated once a day by AWS and contains a list of all keys in the source bucket, stored
# in a collection of Parquet files.  We have to first read a manifest.json file to get the list
# of Parquet files, then read each Parquet file to get the list of keys, and filter them based on
# the section IDs and action. 
def list_keys_from_inventory(section_ids, action, inventoried_bucket_name, bucket_name): 
   
    s3_client = boto3.client('s3')
    
    try:        
        # Read the JSON contents of the most recent inventory manifest file
        manifest_json = get_most_recent_manifest(inventoried_bucket_name, bucket_name)

        if manifest_json is None:
            raise FileNotFoundError("No inventory manifest found in the last two days")

        # Iterate over the Parquet files in the manifest, fetching and reading each one
        # to get the list of keys, and filtering them based on the section IDs and action
        all = []
        for i in range(len(manifest_json["files"])):
            key = manifest_json["files"][i]["key"]
            results = fetch_parquet(section_ids, action, s3_client, bucket_name, key)
            all.extend(results)
            
        return all
        
    except FileNotFoundError:
        # Bubble up so the job fails loudly instead of silently returning no data
        raise
    except Exception as e:
        print(e)
        return []
    
# This function will return the most recent manifest file from the inventory bucket
# It first looks for yesterday's manifest, and if that does not exist, looks for the day prior.
#
# The time lag is due to the fact that the inventory files in AWS are generated once a day.
def get_most_recent_manifest(inventoried_bucket_name, bucket_name):
    
    attempted_keys = []
    for i in range(1, 3):  # yesterday, then two days ago
        day = (datetime.datetime.utcnow() - datetime.timedelta(days=i)).strftime('%Y-%m-%dT01-00Z')
        manifest_key = f'{inventoried_bucket_name}/{bucket_name}/{day}/manifest.json'
        attempted_keys.append(manifest_key)
        
        try:
            s3_client = boto3.client('s3')
            response = s3_client.get_object(Bucket=bucket_name, Key=manifest_key)
            manifest_content = response['Body'].read().decode('utf-8')

            return json.loads(manifest_content)
        
        except Exception as e:
            print(e)
            continue
    
    raise FileNotFoundError(f"No inventory manifest found for keys: {attempted_keys}")
    
    
def fetch_parquet(section_ids, action, s3_client, bucket_name, key):
    
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    
    # Read the Parquet file content
    parquet_file_content = response['Body'].read()

    # Use Pandas to read the Parquet file content
    parquet_buffer = io.BytesIO(parquet_file_content)
    df = pd.read_parquet(parquet_buffer)

    key_values = df['key'].tolist()

    # Create the regex pattern to match the desired keys
    section_pattern = '|'.join(map(lambda x: re.escape(str(x)), section_ids))
    pattern = rf'section/({section_pattern})/{action}/'

    # Filter the DataFrame
    filtered_df = df[df['key'].str.contains(pattern)]
    key_values = filtered_df['key'].tolist()

    return key_values

# This function lists all keys in a bucket that match the specified section ID and action
# It uses the S3 client to list all objects in the bucket, and filters them based on the
# section ID and action. This is not as efficient as using the inventory, but can be used
# when the inventory is not available - or when consistency is of higher concern.
def list_keys(bucket_name, section_id, action):
    
    # Create a session using the specified profile
    
    s3_client = boto3.client('s3')
    
    files = []
    
    prefix = f"section/{section_id}/{action}/"

    # Pagination handling
    continuation_token = None

    while True:
        # List objects with pagination
        if continuation_token:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, ContinuationToken=continuation_token)
        else:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        # Check if the response contains 'Contents'
        if 'Contents' in response:
            # Append the keys to the list
            files.extend([obj['Key'] for obj in response['Contents']])

        # Check if there is a continuation token
        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
        else:
            break
            
    return files
