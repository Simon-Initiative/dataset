import json 

def build_json_manifest(s3_client, context, num_chunks, extension):

    bucket = context["bucket_name"]
    job_id = context["job_id"]
    prefix = f'https://{bucket}.s3.us-east-1.amazonaws.com/'

    # Create a JSON object with the job ID and the list of chunks 
    # as URLs to the S3 objects
    manifest = {
        "context": context,
        "chunks": [f"{prefix}{job_id}/chunk_{i}.{extension}" for i in range(num_chunks)]
    }

    # upload the manifest to S3 into the job_id directory:
    manifest_key = f'{job_id}/manifest.json'
    s3_client.put_object(Bucket="torus-datasets-prod", Key=manifest_key, Body=json.dumps(manifest))


    return manifest_key

def build_html_manifest(s3_client, context, num_chunks, extension):

    bucket = context["bucket_name"]
    job_id = context["job_id"]
    prefix = f'https://{bucket}.s3.us-east-1.amazonaws.com/'

    # Create an HMTL string with a list of files
    html = "<!doctype html><html dir=\"ltr\" lang=\"en\"><head><title>Job Manifest</title></head><body><h1>Job Manifest</h1>"

    # For each key in context, create a row in the table
    html += "<p>Configuration:</p>"
    html += "<table>"

    for key, value in context.items():
        html += f"<tr><td>{key}</td><td>{value}</td></tr>"
    
    html += "</table>\n"

    html += "<ul>"
    for i in range(num_chunks):
        html += f'<li><a href="{prefix}{job_id}/chunk_{i}.{extension}">{prefix}{job_id}/chunk_{i}.{extension}</a></li>'

    html += "</ul></body></html>"

    # upload the manifest to S3 into the job_id directory
    manifest_key = f'{job_id}/index.html'
    s3_client.put_object(Bucket="torus-datasets-prod", Key=manifest_key, Body=html)

    return manifest_key