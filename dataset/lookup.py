import json

def retrieve_lookup(s3_client, context):
    """
    Downloads the lookup JSON file from S3 and returns the content as a Python dictionary.
    The context file is expected to be in the 'contexts' directory in the S3 bucket.
    The key of the context file is the job ID.

    This file provides additional context information for the dataset generation process, allowing
    the dataset generation process to do things like map resource ids to titles, user ids to emails, etc.
    """

    key = context['job_id']
    file_name = f"contexts/{key}.json"

    response = s3_client.get_object(Bucket="torus-datasets-prod", Key=file_name)
    content = response['Body'].read().decode('utf-8')

    parsed = json.loads(content)
    return post_process(parsed)


def determine_student_id(context, json):
    if context['anonymize']:
        return json["actor"]["account"]["name"]
    else:
        
        # Retrieve the user's email from the lookup context, but falling
        # back to the user_id if the user is not found, or email is not available
        user_id = json["actor"]["account"]["name"]
        user_id_str = str(user_id)

        email = context['lookup']['users'].get(user_id_str, {}).get('email', user_id)
        
        return email


def calculate_ancestors(context):

    # We are given the hierarchy like this:
    #'hierarchy': {
    #            '152914': {'graded': True, 'title': 'Assessment 1'},
    #            '24': {'title': 'Unit 1', 'children': [25]},
    #            '25': {'title': 'Module 1', 'children': [152914]},
    #        },
    # 
    # And this calculates an 'ancestors' list for each item in the hierarchy
    # where the ancestors are the parents of the item

    hierarchy = context['hierarchy']

    # calculate the direct parent of each item and store it as 'parent'
    for key, value in hierarchy.items():
        for child in value.get('children', []):
            hierarchy[str(child)]['parent'] = key

    # calculate the ancestors of each item, by walking up the tree of parents
    # until we reach a node that has no parent
    for key, value in hierarchy.items():

        ancestors = []
        current = key

        while 'parent' in hierarchy[current]:
            current = hierarchy[current]['parent']
            ancestors.append(int(current))

        # reverse the ancestors
        ancestors.reverse()
        hierarchy[key]['ancestors'] = ancestors

def mapify_parts(context):
    activities = context['activities']
    
    for activity_id, activity in activities.items():
        parts = activity.get('parts', [])
        activity['parts'] = {}
        # if parts is an array and not None
        if parts is not None and isinstance(parts, list):
            for part in parts:
                # if part is an object:
                if isinstance(part, dict):
                    # if part has an 'id' field:
                    part_id = part.get('id', None)

                    if part_id is not None:
                        # add the part to the parts map
                        activity['parts'][part_id] = part
                
def post_process(context):
    mapify_parts(context)
    calculate_ancestors(context)
    return context