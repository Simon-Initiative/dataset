from dataset.attempts import attempts_handler
from dataset.page_viewed import page_viewed_handler
from dataset.video import video_handler

attempts_columns = [
            "event_type",
            "timestamp",
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

page_viewed_columns = [
            "event_type",
            "timestamp",
            "user_id", 
            "section_id", 
            "project_id", 
            "publication_id", 
            "page_id",
            "page_attempt_guid", 
            "page_attempt_number"
        ]

video_columns = [
    "event_type",
    "timestamp",
    "user_id", 
    "section_id", 
    "project_id", 
    "publication_id", 
    "page_id",
    "page_attempt_guid",
    "page_attempt_number",
    "content_element_id",
    "video_url",
    "video_title",
    "video_length",
    "video_time_from",
    "video_time_to",
    "video_played_segments",
    "video_progress"
]


registered_events = {
    "attempt_evaluated": (attempts_handler, attempts_columns),
    "page_viewed": (page_viewed_handler, page_viewed_columns),
    "video": (video_handler, video_columns)
}

def get_event_config(event):
    return registered_events[event]