import json
import boto3

from dataset.utils import prune_fields

def video_handler(bucket_key, context, excluded_indices):
    # Use the key to read in the file contents, split on line endings
    bucket_name, key = bucket_key

    # Create a session using the specified profile
    s3_client = boto3.client('s3')
    
    response = s3_client.get_object(Bucket=bucket_name, Key=key)

    # Read the contents of the file
    content = response['Body'].read().decode('utf-8')

    values = []

    subtypes = context["sub_types"]
    
    for line in content.splitlines():
        # parse one line of json
        j = json.loads(line)

        student_id = j["actor"]["account"]["name"]
        short_verb = j["verb"]["display"]["en-US"]    

        project_matches = context["project_id"] is None or context["project_id"] == j["context"]["extensions"]["http://oli.cmu.edu/extensions/project_id"]
        page_matches = context["page_ids"] is None or j["context"]["extensions"]["http://oli.cmu.edu/extensions/resource_id"] in context["page_ids"]

        if student_id not in context["ignored_student_ids"] and project_matches and page_matches:
            if short_verb in subtypes:
                if short_verb == "played":
                    o = from_played(j)
                    o = prune_fields(o, excluded_indices)
                    values.append(o)
                elif short_verb == "paused":
                    o = from_paused(j)
                    o = prune_fields(o, excluded_indices)
                    values.append(o)
                elif short_verb == "seeked":
                    o = from_seeked(j)
                    o = prune_fields(o, excluded_indices)
                    values.append(o)
                elif short_verb == "completed":
                    o = from_completed(j)
                    o = prune_fields(o, excluded_indices)
                    values.append(o)
            
        
    return values
        

def from_played(value):
    return [
        "played",
        value["timestamp"],
        value["actor"]["account"]["name"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/section_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/project_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/publication_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/resource_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_attempt_guid"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_attempt_number"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/content_element_id"],
        value["object"]["id"],
        value["object"]["definition"]["name"]["en-US"],
        value["context"]["extensions"]["https://w3id.org/xapi/video/extensions/length"],
        value["result"]["extensions"]["https://w3id.org/xapi/video/extensions/time"],
        None,
        None,
        None
    ]

def from_paused(value):
    return [
        "paused",
        value["timestamp"],
        value["actor"]["account"]["name"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/section_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/project_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/publication_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/resource_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_attempt_guid"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_attempt_number"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/content_element_id"],
        value["object"]["id"],
        value["object"]["definition"]["name"]["en-US"],
        value["context"]["extensions"]["https://w3id.org/xapi/video/extensions/length"],
        value["result"]["extensions"]["https://w3id.org/xapi/video/extensions/time"],
        None,
        value["result"]["extensions"]["https://w3id.org/xapi/video/extensions/played-segments"],
        value["result"]["extensions"]["https://w3id.org/xapi/video/extensions/progress"]
    ]

def from_seeked(value):
    return [
        "seeked",
        value["timestamp"],
        value["actor"]["account"]["name"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/section_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/project_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/publication_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/resource_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_attempt_guid"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_attempt_number"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/content_element_id"],
        value["object"]["id"],
        value["object"]["definition"]["name"]["en-US"],
        value["context"]["extensions"]["https://w3id.org/xapi/video/extensions/length"],
        value["result"]["extensions"]["https://w3id.org/xapi/video/extensions/time-to"],
        value["result"]["extensions"]["https://w3id.org/xapi/video/extensions/time-from"],
        None,
        None
    ]

def from_completed(value):
    return [
        "completed",
        value["timestamp"],
        value["actor"]["account"]["name"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/section_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/project_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/publication_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/resource_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_attempt_guid"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_attempt_number"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/content_element_id"],
        value["object"]["id"],
        value["object"]["definition"]["name"]["en-US"],
        value["context"]["extensions"]["https://w3id.org/xapi/video/extensions/length"],
        value["result"]["extensions"]["https://w3id.org/xapi/video/extensions/time"],
        None,
        value["result"]["extensions"]["https://w3id.org/xapi/video/extensions/played-segments"],
        value["result"]["extensions"]["https://w3id.org/xapi/video/extensions/progress"]
    ]
