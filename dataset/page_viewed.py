import json
import boto3

from dataset.utils import prune_fields

def page_viewed_handler(bucket_key, context, excluded_indices):
    # Use the key to read in the file contents, split on line endings
    bucket_name, key = bucket_key

    # Create a session using the specified profile
    s3_client = boto3.client('s3')
    
    response = s3_client.get_object(Bucket=bucket_name, Key=key)

    # Read the contents of the file
    content = response['Body'].read().decode('utf-8')

    values = []
    
    for line in content.splitlines():
        # parse one line of json
        j = json.loads(line)

        student_id = j["actor"]["account"]["name"]
        
        if student_id not in context["ignored_student_ids"]:
            o = from_page_viewed(j)
            o = prune_fields(o, excluded_indices)
            values.append(o)
            
    return values
        
def from_page_viewed(value):
    return [
        "page_viewed",
        value["timestamp"],
        value["actor"]["account"]["name"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/section_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/project_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/publication_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_attempt_guid"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_attempt_number"]
    ]
