import json
import logging
import boto3

from dataset.utils import encode_array, encode_json, prune_fields
from dataset.lookup import determine_student_id

def attempts_handler(bucket_key, context, excluded_indices):
    """
    Entry point for attempts extraction. Any exception is swallowed so the
    Spark job cannot be aborted by a bad record or failed download.
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

        subtypes = context["sub_types"]

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
                    if (("part_attempt_evaluated" in subtypes) or ("part_attempt_evaluted" in subtypes)) and j["object"]["definition"]["type"] == "http://adlnet.gov/expapi/activities/question":
                        o = from_part_attempt(j, context)
                        o = prune_fields(o, excluded_indices)
                        values.append(o)
                    elif "activity_attempt_evaluated" in subtypes and j["object"]["definition"]["type"] == "http://oli.cmu.edu/extensions/activity_attempt":
                        o = from_activity_attempt(j, context)
                        o = prune_fields(o, excluded_indices)
                        values.append(o)
                    elif (("page_attempt_evaluated" in subtypes) or ("page_attempt_evaluted" in subtypes)) and j["object"]["definition"]["type"] == "http://oli.cmu.edu/extensions/page_attempt":
                        o = from_page_attempt(j, context)
                        o = prune_fields(o, excluded_indices)
                        values.append(o)
            except Exception:
                logging.exception("attempts_handler: skipping malformed line in %s", key)
                continue

        return values

    except Exception:
        logging.exception("attempts_handler: failed to process %s", bucket_key)
        debug_log(context, f"Failed to process {bucket_key}")
        return []
        

def from_part_attempt(value, context):
    return [
        value["object"]["definition"]["name"]["en-US"],
        value["timestamp"],
        determine_student_id(context, value),
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

def from_activity_attempt(value, context):
    return [
        value["object"]["definition"]["name"]["en-US"],
        value["timestamp"],
        determine_student_id(context, value),
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

def from_page_attempt(value, context):
    return [
        value["object"]["definition"]["name"]["en-US"],
        value["timestamp"],
        determine_student_id(context, value),
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


def debug_log(context, message):
    """Log a debug message if debugging is enabled in the context."""
    if context.get("debug", False):
        print(f"DEBUG: {message}")

