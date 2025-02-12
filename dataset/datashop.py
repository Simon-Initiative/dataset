import xml.etree.ElementTree as ET
import json
import boto3
import random 
import string

from dataset.lookup import determine_student_id


def handle_datashop(bucket_key, context, excluded_indices):
    bucket_name, key = bucket_key

    # Create a session using the specified profile
    s3_client = boto3.client('s3')
    
    response = s3_client.get_object(Bucket=bucket_name, Key=key)

    # Read the contents of the file
    content = response['Body'].read().decode('utf-8')

    values = []

    lookup = context['lookup']

    for line in content.splitlines():
        # parse one line of json
        j = json.loads(line)

        student_id = j["actor"]["account"]["name"]
        project_matches = context["project_id"] is None or context["project_id"] == j["context"]["extensions"]["http://oli.cmu.edu/extensions/project_id"]
        
        if student_id not in context["ignored_student_ids"] and project_matches:
            if j["object"]["definition"]["type"] == "http://adlnet.gov/expapi/activities/question":
                o = to_xml_message(j, lookup)
                values.append(o)
            
    return values

def to_xml_message(json, context): 

    part_attempt = parse_attempt(json, context)
    part_attempt['activity_type'] = context['activities'].get(str(part_attempt['activity_id']), {'type': 'Unknown'})['type']

    context = expand_context(context, part_attempt)

    c_message = context_message("START_PROBLEM", context)
    hint_message_pairs = create_hint_message_pairs(part_attempt, context)

    # Attempt / Result pairs must have a different transaction ID from the hint message pairs
    context["transaction_id"] = unique_id(part_attempt)

    all = [c_message] + hint_message_pairs + [
        tool_message("ATTEMPT", "ATTEMPT", context),
        tutor_message("RESULT", context)
    ]

    # concatenate all the messages to a single string
    return "\n".join(all)


def expand_context(context, part_attempt):

    datashop_session_id = part_attempt['datashop_session_id'] if 'datashop_session_id' in part_attempt else today(part_attempt)
    problem_name = f"Activity {part_attempt['activity_id']}, Part {part_attempt['part_id']}"

    activity_id = part_attempt['activity_id']
    part_id = part_attempt['part_id']

    activity = context['activities'].get(str(activity_id), {'parts': {part_id: {'hints': []}}})
    parts = activity.get('parts', {'parts': {}})
    part = parts.get(part_id, {'hints': []})

    hints = part.get('hints', [])

    if not hints or not isinstance(hints, list):
        hints = []

    hint_text = [get_text_from_content(h) for h in hints]

    # count the nubmer of hints that are not empty strings:
    total_hints_available = len([h for h in hint_text if h])

    expanded = {
        'time': part_attempt['timestamp'],
        'user_id': str(part_attempt['user_id']),
        'session_id': datashop_session_id,
        'datashop_session_id': datashop_session_id,
        'context_message_id': unique_id(part_attempt),
        'activity_slug': str(activity_id),
        'problem_name': problem_name,
        'transaction_id': unique_id(part_attempt),
        'dataset_name': context['dataset_name'],
        'part_attempt': part_attempt,
        'hierarchy': context['hierarchy'],
        'activities': context['activities'],
        'skill_titles': context['skill_titles'],
        'skill_ids': part_attempt['attached_objectives'],
        'total_hints_available': total_hints_available
    }
    context.update(expanded)

    return context


def unique_id(part_attempt):
    return f"{part_attempt['activity_id']}-part{part_attempt['part_id']}-{random_string(8)}"

def random_string(length):
    return ''.join(random.choice(string.ascii_letters) for i in range(length))

def today(part_attempt):
    # Build a session id that is today's date from teh timestamp + the user _id

    # take the timestamp and get the date ("2024-09-02T18:24:33Z") -> "2024-09-02"
    timestamp = part_attempt['timestamp']

    # trim after the T
    date = timestamp.split('T')[0]
    return date + '-' + str(part_attempt['user_id'])
   
def parse_attempt(value, context):
    return {
        'timestamp': value["timestamp"],
        'user_id': determine_student_id(context, value),
        'section_id': value["context"]["extensions"]["http://oli.cmu.edu/extensions/section_id"],
        'project_id': value["context"]["extensions"]["http://oli.cmu.edu/extensions/project_id"],
        'publication_id': value["context"]["extensions"]["http://oli.cmu.edu/extensions/publication_id"],
        'page_id': value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_id"],
        'activity_id': value["context"]["extensions"]["http://oli.cmu.edu/extensions/activity_id"],
        'activity_revision_id': value["context"]["extensions"]["http://oli.cmu.edu/extensions/activity_revision_id"],
        'attached_objectives': value["context"]["extensions"]["http://oli.cmu.edu/extensions/attached_objectives"],
        'page_attempt_guid': value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_attempt_guid"],
        'page_attempt_number': value["context"]["extensions"]["http://oli.cmu.edu/extensions/page_attempt_number"],
        'part_id': value["context"]["extensions"]["http://oli.cmu.edu/extensions/part_id"],
        'part_attempt_guid': value["context"]["extensions"]["http://oli.cmu.edu/extensions/part_attempt_guid"],
        'part_attempt_number': value["context"]["extensions"]["http://oli.cmu.edu/extensions/part_attempt_number"],        
        'activity_attempt_number': value["context"]["extensions"]["http://oli.cmu.edu/extensions/activity_attempt_number"],
        'activity_attempt_guid': value["context"]["extensions"]["http://oli.cmu.edu/extensions/activity_attempt_guid"],
        'attached_objectives': value["context"]["extensions"]["http://oli.cmu.edu/extensions/attached_objectives"],
        'score': value["result"]["score"]["raw"], #score
        'out_of': value["result"]["score"]["max"], #out_of
        'hints': value["context"]["extensions"]["http://oli.cmu.edu/extensions/hints_requested"],
        'response': value["result"]["response"], # response
        'feedback': value["result"]["extensions"]["http://oli.cmu.edu/extensions/feedback"], #feedback
    }

def create_hint_message_pairs(part_attempt, context):
    """
    Creates a list of hint messages for the part_attempt.
    """
    hints = get_hints_for_part(part_attempt, context)

    # hints is simply a list of strings

    hint_message_pairs = []

    for hint_index, hint_text in enumerate(hints):
        hint_context = {
            "date": part_attempt["timestamp"],
            "current_hint_number": hint_index + 1,
            "hint_text": hint_text
        }

        # merge hint_context with context
        hint_context.update(context)

        tool_hint = tool_message("HINT", "HINT_REQUEST", hint_context)
        tutor_hint = tutor_message("HINT_MSG", hint_context)
        hint_message_pairs.extend([tool_hint, tutor_hint])

    return hint_message_pairs

def make_unique_id(activity_slug, part_id):
    """
    Creates a unique ID from the activity_slug and part_id.
    """
    return f"{activity_slug}-{part_id}"

def get_hints_for_part(part_attempt, context):
    """
    Retrieves hints for the part_attempt.
    """
    hints = part_attempt.get("hints", [])
    part_id = part_attempt.get("part_id")
    activity_id = part_attempt.get("activity_id")

    text = []

    # create of id map of hints
    activity = context["activities"].get(str(activity_id), {'parts': {part_id: {'hints': []}}})
    parts = activity.get('parts', {'parts': []})
    if parts is None or not isinstance(parts, dict):
        parts = {}
    part = parts.get(part_id, {'hints': []})
    if part is None or not isinstance(part, dict):
        part = {}
    all_hints = part.get('hints', [])
    if all_hints is None or not isinstance(all_hints, list):
        all_hints = []

    hint_map = {hint["id"]: hint for hint in all_hints}

    for hint in hints:
        h = hint_map.get(hint, {'content': [{'text': 'Unknown hint'}]})
        text.append(get_text_from_content(h))

    return text


def tutor_message(message_type, context):
    """
    Creates a <tutor_message> XML element with nested <meta>, <problem_name>, <semantic_event>,
    <event_descriptor>, <action_evaluation>, and optionally <tutor_advice> and <skills>.
    """
    tutor_message_elem = ET.Element("tutor_message", {
        "context_message_id": context.get("context_message_id", "Unknown")
    })

    # Add nested elements
    tutor_message_elem.append(meta(context))
    tutor_message_elem.append(problem_name(context))
    tutor_message_elem.append(semantic_event(message_type, context))
    tutor_message_elem.append(event_descriptor(message_type, context))
    tutor_message_elem.append(action_evaluation(context))

    # Conditionally add <tutor_advice> if message_type is "HINT_MSG"
    if message_type == "HINT_MSG":
        tutor_message_elem.append(tutor_advice(context))

    # Add <skills>
    tutor_message_elem.extend(skills(context))

    return ET.tostring(tutor_message_elem, encoding="unicode")


def tool_message(event_descriptor_type, semantic_event_type, context):
    """
    Creates a <tool_message> XML element with nested <meta>, <problem_name>, <semantic_event>, and <event_descriptor>.
    """
    tool_message_elem = ET.Element("tool_message", {
        "context_message_id": context.get("context_message_id", "Unknown")
    })

    # Add nested elements
    tool_message_elem.append(meta(context))
    tool_message_elem.append(problem_name(context))
    tool_message_elem.append(semantic_event(semantic_event_type, context))
    tool_message_elem.append(event_descriptor(event_descriptor_type, context))

    return ET.tostring(tool_message_elem, encoding="unicode")


def context_message(name, context):
    """
    Creates a <context_message> XML element with nested <meta> and <dataset> elements.
    """
    context_elem = ET.Element(
        "context_message",
        attrib={
            "context_message_id": context.get("context_message_id", "Unknown"),
            "name": name,
        },
    )

    # Add <meta> and <dataset> elements
    context_elem.append(meta(context))
    context_elem.append(dataset(context))

    return ET.tostring(context_elem, encoding="unicode")



def tutor_advice(context):
    """
    Creates a <tutor_advice> XML element with the provided hint_text.
    """
    tutor_advice_elem = ET.Element("tutor_advice")
    tutor_advice_elem.text = context.get("hint_text", "Unknown Hint")
    return tutor_advice_elem


def skills(context):
    """
    Creates a list of <skill> elements.
    """
    skill_ids = context.get("skill_ids", [])
    skill_titles = context.get("skill_titles", {})
    skill_elements = []

    for skill_id in skill_ids:
        skill_title = skill_titles.get(str(skill_id), "Unknown")
        skill_elem = ET.Element("skill")
        ET.SubElement(skill_elem, "name").text = skill_title
        skill_elements.append(skill_elem)

    return skill_elements


def semantic_event(event_type, context):
    """
    Creates a <semantic_event> XML element with transaction_id and name attributes.
    """
    semantic_event_elem = ET.Element("semantic_event", {
        "transaction_id": context.get("transaction_id", "Unknown"),
        "name": event_type
    })
    return semantic_event_elem


def problem_name(context):
    """
    Creates a <problem_name> XML element with appropriate text based on context.
    """
    activity_slug = context.get("activity_slug")
    part_id = context.get("part_id")
    problem_name = context.get("problem_name")

    if problem_name:
        text = problem_name
    elif activity_slug and part_id:
        text = f"Activity {activity_slug}, part {part_id}"
    else:
        text = "Unknown"

    element = ET.Element("problem_name")
    element.text = text
    return element


def meta(context):
    """
    Creates the <meta> element for the context message.
    """
    meta_elem = ET.Element("meta")
    ET.SubElement(meta_elem, "user_id").text = context.get("user_id", "Unknown")
    ET.SubElement(meta_elem, "session_id").text = context.get("session_id", "Unknown")
    ET.SubElement(meta_elem, "time").text = format_time(context.get("time"))
    ET.SubElement(meta_elem, "time_zone").text = context.get("time_zone", "GMT")

    return meta_elem


def format_time(time_obj):
    """
    Formats a datetime object into "YYYY-MM-DD HH:MM".
    """

    # time obj will be of this form '2024-09-02T18:24:33Z'
    # we want to return '2024-09-02 18:24:33'
    return time_obj.split('T')[0] + ' ' + time_obj.split('T')[1].split('Z')[0]

def format_date(time_obj):
    """
    Formats a datetime object into "YYYY-MM-DD HH:MM:SS".
    If `date_obj` is not a valid datetime, returns "Unknown".
    """
    # time obj will be of this form '2024-09-02T18:24:33Z'
    # we want to return '2024-09-02 18:24:33'
    return time_obj.split('T')[0] + ' ' + time_obj.split('T')[1].split('Z')[0]
    


def action_evaluation(context):
    """
    Creates an <action_evaluation> XML element based on context.
    """
    current_hint_number = context.get("current_hint_number")
    total_hints_available = context.get("total_hints_available")
    part_attempt = context.get("part_attempt")

    if current_hint_number is not None and total_hints_available is not None:
        element = ET.Element("action_evaluation", attrib={
            "current_hint_number": str(current_hint_number),
            "total_hints_available": str(total_hints_available)
        })
        element.text = "HINT"
    elif part_attempt:
        element = ET.Element("action_evaluation")
        element.text = correctness(part_attempt)
    else:
        raise ValueError("Invalid context: Missing hint details or part_attempt.")

    return element


def event_descriptor(event_type, context):
    """
    Creates an <event_descriptor> XML element based on context.
    """
    selection = create_element("selection", context["problem_name"])
    action = create_element("action", get_action(context.get("part_attempt")))
    input_ = create_element("input", get_input(event_type, context))

    event_descriptor_elem = ET.Element("event_descriptor")
    event_descriptor_elem.extend([selection, action, input_])

    return event_descriptor_elem

def correctness(part_attempt):
    """
    Determines correctness based on part_attempt:
    - "CORRECT" if score equals out_of.
    - "INCORRECT" otherwise.
    """
    return "CORRECT" if part_attempt.get("score") == part_attempt.get("out_of") else "INCORRECT"


def get_action(part_attempt):
    """
    Maps activity types to specific action names.
    """
    type_map = {
        "oli_short_answer": "Short answer submission",
        "oli_multiple_choice": "Multiple choice submission",
        "oli_check_all_that_apply": "Check all that apply submission",
        "oli_ordering": "Ordering submission",
        "oli_multi_input": "Multi input submission",
        "oli_response_multi": "Response multi submission",
        "oli_image_coding": "Image coding submission",
        "oli_adaptive": "Adaptive submission",
    }
    activity_type = get_activity_type(part_attempt)
    return type_map.get(activity_type, "Activity submission")


def get_input(event_type, context):
    """
    Retrieves the appropriate input for the event based on type.
    """
    part_attempt = context.get("part_attempt")
    if event_type in {"HINT", "HINT_MSG"}:
        return "HINT"
    elif event_type == "ATTEMPT":
        input_ = handle_input_by_activity(context)
        return input_[:255] if input_ else "Student input"
    elif event_type == "RESULT":
        return select_feedback(part_attempt)
    return "Student input"


def handle_input_by_activity(context):
    """
    Handles input based on activity type.
    """
    part_attempt = context.get("part_attempt")
    response = part_attempt.get("response", {})
    
    if response and isinstance(response, dict):
        input_ = response.get("input", "")
    else:
        input_ = ""

    activity_type = get_activity_type(part_attempt)

    handlers = {
        "oli_short_answer": lambda c, x: x,
        "oli_multiple_choice": choices_input,
        "oli_check_all_that_apply": choices_input,
        "oli_ordering": choices_input,
        "oli_multi_input": multi_input_handler,
        "oli_response_multi": multi_input_handler,
        "oli_image_coding": lambda c, x: x,
        "oli_adaptive": lambda c, x: "Adaptive Activity",
        "oli_likert": multi_input_handler, # same as multi_input, we pull from choices but fall back to original value
        "oli_directed_discussion": lambda c, x: "Directed Discussion",
    }
    return handlers.get(activity_type, lambda c, x: "Unknown Activity")(context, input_)


def multi_input_handler(context, input):
    """
    Handles multi-input cases.
    """

    value = choices_input(context, input)

    if value == 'Unknown Choice':
        return input
    else:
        return value


def choices_input(context, input_):
    """
    Handles input for choice-based activities.
    """
    activity_id = context["part_attempt"]["activity_id"]

    activity = context['activities'].get(str(activity_id), {'choices': []})
    
    choices = activity.get("choices", [])

    if not choices or not isinstance(choices, list):
        choices = []

    # choices is alist of dicts with keys 'id' and 'content', turn it 
    # into a dict with 'id' as key and 'content' as value
    choices = {choice['id']: choice for choice in choices}

    # trim input_, then split it by spaces
    if input_ is None:
        return "Unknown Choice"
    else:
        input_ = input_.strip().split(" ")

        input_ = [choices.get(choice, {'content': [{'text': 'Unknown Choice'}]}) for choice in input_]

        # convert the array of {'content': [{'text': 'Choice'}]} to an array of strings and contactenate them
        items = [get_text_from_content(choice) for choice in input_]
        return " ".join(items)


def get_activity_type(part_attempt):
    """
    Gets the activity type slug from activity_types.
    """
    return part_attempt["activity_type"]


def select_feedback(part_attempt):
    """
    Selects feedback from part_attempt.  The feedback is a JSON blob. 

    This function gets the "content" field from the feedback, then recursively procecesses 
    the entrie and children to get ALL "text" attributes, concatenating them into a single string.
    """
    feedback = part_attempt.get("feedback", {"content": [{"text": "No Feedback"}]})

    return get_text_from_content(feedback)

def get_text_from_content(item):

    if item is None:
        return ""
    else:
        def extract_text(content):
            text = ""
            for item in content:
                if isinstance(item, dict):
                    if item.get("text"):
                        text += item["text"]
                    if item.get("children"):
                        text += extract_text(item["children"])
                else:
                    text += ""
                    
            return text
        
        return extract_text(item.get("content", []))

def create_element(tag, text):
    """
    Helper function to create an XML element with text content.
    """
    element = ET.Element(tag)
    element.text = text
    return element


def dataset(context):
    """
    Creates the <dataset> element for the context message.
    """
    dataset_elem = ET.Element("dataset")
    ET.SubElement(dataset_elem, "name").text = context.get("dataset_name", "Unknown")
    problem_hierarchy = create_problem_hierarchy(context)
    dataset_elem.append(problem_hierarchy)

    return dataset_elem


def create_problem_hierarchy(context):
    """
    Creates the problem hierarchy as nested XML elements.
    """
    part_attempt = context.get("part_attempt")
    problem_name = context.get("problem_name")
    hierarchy = context.get("hierarchy")

    # Determine the target resource ID
    page_id = part_attempt["page_id"]

    return assemble_from_hierarchy_path(page_id, problem_name, hierarchy)



def assemble_from_hierarchy_path(page_id, problem_name, hierarchy):
    """
    Assembles nested XML elements from the hierarchy path.
    """
    def page_to_element(revision):
        level_elem = ET.Element("level", {"type": "Page"})
        ET.SubElement(level_elem, "name").text = revision["title"]
        problem_elem = ET.SubElement(level_elem, "problem", {"tutorFlag": tutor_or_test(revision["graded"])})
        ET.SubElement(problem_elem, "name").text = problem_name
        return level_elem

    def container_to_element(revision, child):
        container_elem = ET.Element("level", {"type": "Container"})
        ET.SubElement(container_elem, "name").text = revision["title"]
        container_elem.append(child)
        return container_elem

    page = hierarchy.get(str(page_id), {"title": "Unknown Page", "ancestors": [], "graded": False})

    child = page_to_element(page)

    for a in reversed(page['ancestors']):
        child = container_to_element(hierarchy[str(a)], child)

    return child


def tutor_or_test(graded):
    """
    Determines whether the problem is a tutor or test problem.
    """
    return "test" if graded else "tutor"