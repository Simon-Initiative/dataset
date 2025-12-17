import json
import boto3

from dataset.utils import prune_fields
from dataset.lookup import determine_student_id

def page_viewed_handler(bucket_key, context, excluded_indices):
    """
    Entry point for page viewed extraction. Never raise so Spark job keeps going.
    """
    try:
        # Use the key to read in the file contents, split on line endings
        bucket_name, key = bucket_key

        # Create a session using the specified profile
        s3_client = boto3.client('s3')
        
        response = s3_client.get_object(Bucket=bucket_name, Key=key)

        # Read the contents of the file
        content = response['Body'].read().decode('utf-8')

        values = []
        
        for line in content.splitlines():
            if not line.strip():
                continue
            try:
                # parse one line of json
                j = json.loads(line)

                student_id = j["actor"]["account"]["name"]
                project_matches = context["project_id"] is None or context["project_id"] == j["context"]["extensions"]["http://oli.cmu.edu/extensions/project_id"]
                page_matches = context["page_ids"] is None or j["context"]["extensions"]["http://oli.cmu.edu/extensions/page_id"] in context["page_ids"]
                
                if student_id not in context["ignored_student_ids"] and project_matches and page_matches:
                    o = from_page_viewed(j, context)
                    o = prune_fields(o, excluded_indices)
                    values.append(o)
            except Exception as exc:
                debug_log(context, f"page_viewed_handler: skipping malformed line in {key}: {exc}")
                continue
            
        return values

    except Exception as exc:
        debug_log(context, f"page_viewed_handler: failed to process {bucket_key}: {exc}")
        return []
        
def from_page_viewed(value, context):
    return [
        "page_viewed",
        value["timestamp"],
        determine_student_id(context, value),
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/section_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/project_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/publication_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_attempt_guid"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_attempt_number"]
    ]


def debug_log(context, message):
    """Log a debug message if debugging is enabled in the context."""
    if context.get("debug", False):
        print(f"DEBUG: {message}")
