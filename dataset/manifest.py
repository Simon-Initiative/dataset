import json 

def build_json_manifest(s3_client, bucket, job_id, num_chunks):

    prefix = f'https://{bucket}.s3.us-east-1.amazonaws.com/'

    # Create a JSON object with the job ID and the list of chunks 
    # as URLs to the S3 objects

    manifest = {
        "job_id": job_id,
        "chunks": [f"{prefix}{job_id}/chunk_{i}.csv" for i in range(num_chunks)]
    }

    # upload the manifest to S3 into the job_id directory:
    manifest_key = f'{job_id}/manifest.json'
    s3_client.put_object(Bucket=bucket, Key=manifest_key, Body=json.dumps(manifest))

    return manifest_key

def build_html_manifest(s3_client, bucket, job_id, num_chunks):

    prefix = f'https://{bucket}.s3.us-east-1.amazonaws.com/'

    # Create an HMTL string with a list of files
    html = "<!doctype html><html dir=\"ltr\" lang=\"en\"><head><title>Job Manifest</title></head><body><h1>Job Manifest</h1><ul>"
    for i in range(num_chunks):
        html += f'<li><a href="{prefix}{job_id}/chunk_{i}.csv">{prefix}{job_id}/chunk_{i}.csv</a></li>'

    html += "</ul></body></html>"

    # upload the manifest to S3 into the job_id directory
    manifest_key = f'{job_id}/index.html'
    s3_client.put_object(Bucket=bucket, Key=manifest_key, Body=html)

    return manifest_key