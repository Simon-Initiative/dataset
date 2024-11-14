from dataset.attempts import attempts_handler

attempts_columns = [
            "event_type",
            "user_id", 
            "section_id", 
            "project_id", 
            "publication_id", 
            "page_id",
            "activity_id", 
            "activity_revision_id", 
            "attached_objectives",
            "page_attempt_guid", 
            "page_attempt_number", 
            "part_id",
            "part_attempt_guid", 
            "part_attempt_number", 
            "activity_attempt_number", 
            "activity_attempt_guid",
            "score", 
            "out_of", 
            "response", 
            "feedback", 
            "hints"
        ]

registered_events = {
    "attempt_evaluated": (attempts_handler, attempts_columns)
}

def get_event_config(event):
    return registered_events[event]