import json
import boto3

from dataset.utils import encode_array, encode_json

def attempts_handler(bucket_key, context):
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
        
        if student_id not in context["ignored_student_ids"]:
            if "part_attempt_evaluated" in subtypes and j["object"]["definition"]["type"] == "http://adlnet.gov/expapi/activities/question":
                o = from_part_attempt(j)
                values.append(o)
            elif "activity_attempt_evaluated" in subtypes and j["object"]["definition"]["type"] == "http://oli.cmu.edu/extensions/activity_attempt":
                o = from_activity_attempt(j)
                values.append(o)
            elif "page_attempt_evaluated" in subtypes and j["object"]["definition"]["type"] == "http://oli.cmu.edu/extensions/page_attempt":
                o = from_page_attempt(j)
                values.append(o)

        
    return values
        

def from_part_attempt(value):
    return [
        "part_attempt_evaluated",
        value["timestamp"],
        value["actor"]["account"]["name"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/section_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/project_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/publication_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/activity_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/activity_revision_id"],
        encode_array(value["context"]["extensions"]["http://oli.cmu.edu/extensions/attached_objectives"]),
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_attempt_guid"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_attempt_number"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/part_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/part_attempt_guid"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/part_attempt_number"],        
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/activity_attempt_number"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/activity_attempt_guid"],
        value["result"]["score"]["raw"], #score
        value["result"]["score"]["max"], #out_of
        encode_json(value["result"]["response"]), # response
        encode_json(value["result"]["extensions"]["http://oli.cmu.edu/extensions/feedback"]), #feedback
        encode_array(value["context"]["extensions"]["http://oli.cmu.edu/extensions/hints_requested"]), #hints
    ]

def from_activity_attempt(value):
    return [
        "activity_attempt_evaluated",
        value["timestamp"],
        value["actor"]["account"]["name"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/section_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/project_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/publication_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/activity_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/activity_revision_id"],
        None,
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_attempt_guid"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_attempt_number"],
        None,
        None,
        None,        
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/activity_attempt_number"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/activity_attempt_guid"],
        value["result"]["score"]["raw"], #score
        value["result"]["score"]["max"], #out_of
        None, # response
        None, #feedback
        None
    ]

def from_page_attempt(value):
    return [
        "page_attempt_evaluated",
        value["timestamp"],
        value["actor"]["account"]["name"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/section_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/project_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/publication_id"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_id"],
        None,
        None,
        None,
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_attempt_guid"],
        value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_attempt_number"],
        None,
        None,
        None,        
        None,
        None,
        value["result"]["score"]["raw"], #score
        value["result"]["score"]["max"], #out_of
        None, # response
        None, #feedback
        None
    ]
