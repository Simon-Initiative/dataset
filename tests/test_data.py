"""
Test data and mock utilities for comprehensive testing.
"""
import json
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock
import pandas as pd
import io

# Sample xAPI event data
SAMPLE_PART_ATTEMPT_EVENT = {
    "timestamp": "2024-09-02T18:24:33Z",
    "actor": {
        "account": {"name": "12345"}
    },
    "object": {
        "definition": {
            "type": "http://adlnet.gov/expapi/activities/question",
            "name": {"en-US": "part_attempt_evaluated"}
        }
    },
    "context": {
        "extensions": {
            "http://oli.cmu.edu/extensions/section_id": 1001,
            "http://oli.cmu.edu/extensions/project_id": 2001,
            "http://oli.cmu.edu/extensions/publication_id": 3001,
            "http://oli.cmu.edu/extensions/page_id": 4001,
            "http://oli.cmu.edu/extensions/activity_id": 5001,
            "http://oli.cmu.edu/extensions/activity_revision_id": 6001,
            "http://oli.cmu.edu/extensions/attached_objectives": [101, 102],
            "http://oli.cmu.edu/extensions/page_attempt_guid": "page-guid-123",
            "http://oli.cmu.edu/extensions/page_attempt_number": 1,
            "http://oli.cmu.edu/extensions/part_id": "part1",
            "http://oli.cmu.edu/extensions/part_attempt_guid": "part-guid-123",
            "http://oli.cmu.edu/extensions/part_attempt_number": 1,
            "http://oli.cmu.edu/extensions/activity_attempt_number": 1,
            "http://oli.cmu.edu/extensions/activity_attempt_guid": "activity-guid-123",
            "http://oli.cmu.edu/extensions/hints_requested": ["hint1", "hint2"]
        }
    },
    "result": {
        "score": {"raw": 10, "max": 10},
        "response": {"input": "correct answer"},
        "extensions": {
            "http://oli.cmu.edu/extensions/feedback": {
                "content": [{"text": "Well done!"}]
            }
        }
    }
}

SAMPLE_ACTIVITY_ATTEMPT_EVENT = {
    "timestamp": "2024-09-02T18:25:33Z",
    "actor": {
        "account": {"name": "12345"}
    },
    "object": {
        "definition": {
            "type": "http://oli.cmu.edu/extensions/activity_attempt",
            "name": {"en-US": "activity_attempt_evaluated"}
        }
    },
    "context": {
        "extensions": {
            "http://oli.cmu.edu/extensions/section_id": 1001,
            "http://oli.cmu.edu/extensions/project_id": 2001,
            "http://oli.cmu.edu/extensions/publication_id": 3001,
            "http://oli.cmu.edu/extensions/page_id": 4001,
            "http://oli.cmu.edu/extensions/activity_id": 5001,
            "http://oli.cmu.edu/extensions/activity_revision_id": 6001,
            "http://oli.cmu.edu/extensions/page_attempt_guid": "page-guid-123",
            "http://oli.cmu.edu/extensions/page_attempt_number": 1,
            "http://oli.cmu.edu/extensions/activity_attempt_number": 1,
            "http://oli.cmu.edu/extensions/activity_attempt_guid": "activity-guid-123"
        }
    },
    "result": {
        "score": {"raw": 8, "max": 10}
    }
}

SAMPLE_PAGE_VIEWED_EVENT = {
    "timestamp": "2024-09-02T18:20:33Z",
    "actor": {
        "account": {"name": "12345"}
    },
    "object": {
        "definition": {
            "type": "http://adlnet.gov/expapi/activities/page",
            "name": {"en-US": "page_viewed"}
        }
    },
    "context": {
        "extensions": {
            "http://oli.cmu.edu/extensions/section_id": 1001,
            "http://oli.cmu.edu/extensions/project_id": 2001,
            "http://oli.cmu.edu/extensions/publication_id": 3001,
            "http://oli.cmu.edu/extensions/page_id": 4001,
            "http://oli.cmu.edu/extensions/page_attempt_guid": "page-guid-123",
            "http://oli.cmu.edu/extensions/page_attempt_number": 1
        }
    }
}

SAMPLE_VIDEO_PLAYED_EVENT = {
    "timestamp": "2024-09-02T18:30:33Z",
    "actor": {
        "account": {"name": "12345"}
    },
    "verb": {
        "display": {"en-US": "played"}
    },
    "object": {
        "id": "https://example.com/video123",
        "definition": {
            "name": {"en-US": "Sample Video"}
        }
    },
    "context": {
        "extensions": {
            "http://oli.cmu.edu/extensions/section_id": 1001,
            "http://oli.cmu.edu/extensions/project_id": 2001,
            "http://oli.cmu.edu/extensions/publication_id": 3001,
            "http://oli.cmu.edu/extensions/resource_id": 4001,
            "http://oli.cmu.edu/extensions/page_attempt_guid": "page-guid-123",
            "http://oli.cmu.edu/extensions/page_attempt_number": 1,
            "http://oli.cmu.edu/extensions/content_element_id": "video-element-1",
            "https://w3id.org/xapi/video/extensions/length": 300
        }
    },
    "result": {
        "extensions": {
            "https://w3id.org/xapi/video/extensions/time": 45,
            "https://w3id.org/xapi/video/extensions/played-segments": "0[.]45",
            "https://w3id.org/xapi/video/extensions/progress": 0.15
        }
    }
}

# Sample context and lookup data
SAMPLE_CONTEXT = {
    "bucket_name": "test-bucket",
    "inventory_bucket_name": "test-bucket-inventory",
    "results_bucket_name": "test-results-bucket",
    "job_id": "test-job-123",
    "section_ids": [1001, 1002],
    "page_ids": [4001, 4002],
    "ignored_student_ids": [99999],
    "chunk_size": 100,
    "sub_types": ["part_attempt_evaluated"],
    "exclude_fields": [],
    "project_id": 2001,
    "anonymize": False
}

SAMPLE_LOOKUP_DATA = {
    "dataset_name": "Test Dataset",
    "users": {
        "12345": {"email": "student@example.com", "name": "Test Student"}
    },
    "activities": {
        "5001": {
            "type": "oli_multiple_choice",
            "choices": [
                {"id": "choice1", "content": [{"text": "Option A"}]},
                {"id": "choice2", "content": [{"text": "Option B"}]}
            ],
            "parts": {
                "part1": {
                    "hints": [
                        {"id": "hint1", "content": [{"text": "This is hint 1"}]},
                        {"id": "hint2", "content": [{"text": "This is hint 2"}]}
                    ]
                }
            }
        }
    },
    "hierarchy": {
        "4001": {
            "title": "Test Page",
            "graded": True,
            "ancestors": [24, 25]
        },
        "24": {
            "title": "Test Unit",
            "children": [25]
        },
        "25": {
            "title": "Test Module",
            "children": [4001]
        }
    },
    "skill_titles": {
        "101": "Math Skills",
        "102": "Problem Solving"
    }
}

SAMPLE_INVENTORY_MANIFEST = {
    "sourceBucket": "test-bucket",
    "destinationBucket": "test-bucket-inventory", 
    "version": "2016-11-30",
    "creationTimestamp": "1609459200000",
    "fileFormat": "Parquet",
    "fileSchema": "Bucket, Key, Size, LastModifiedDate, ETag, StorageClass, IsMultipartUploaded, ReplicationStatus, EncryptionStatus, ObjectLockRetainUntilDate, ObjectLockMode, ObjectLockLegalHoldStatus, IntelligentTieringAccessTier, BucketKeyStatus, ChecksumAlgorithm",
    "files": [
        {
            "key": "test-bucket-inventory/test-bucket/2024-01-01T01-00Z/inventory.parquet",
            "size": 1024,
            "MD5checksum": "abc123"
        }
    ]
}

def create_mock_s3_client():
    """Create a mock S3 client with common responses."""
    mock_client = Mock()
    
    # Mock get_object for various files
    def mock_get_object(Bucket, Key):
        mock_response = {}
        mock_body = Mock()
        mock_response['Body'] = mock_body
        
        if "manifest.json" in Key:
            mock_body.read.return_value = json.dumps(SAMPLE_INVENTORY_MANIFEST).encode('utf-8')
        elif "inventory.parquet" in Key:
            # Return fake parquet data - actual tests will mock pandas.read_parquet
            mock_body.read.return_value = b'fake_parquet_data'
        elif ".jsonl" in Key:
            # Mock JSONL event data
            events = [SAMPLE_PART_ATTEMPT_EVENT, SAMPLE_ACTIVITY_ATTEMPT_EVENT]
            content = '\n'.join(json.dumps(event) for event in events)
            mock_body.read.return_value = content.encode('utf-8')
        elif "contexts/" in Key:
            mock_body.read.return_value = json.dumps(SAMPLE_LOOKUP_DATA).encode('utf-8')
        else:
            mock_body.read.return_value = b'{"default": "data"}'
        return mock_response
    
    mock_client.get_object = Mock(side_effect=mock_get_object)
    mock_client.put_object = Mock()
    mock_client.list_objects_v2 = Mock(return_value={
        'Contents': [
            {'Key': 'section/1001/attempt_evaluated/file1.jsonl'},
            {'Key': 'section/1001/page_viewed/file2.jsonl'}
        ]
    })
    
    return mock_client

def create_mock_spark_context():
    """Create a mock Spark context."""
    mock_sc = Mock()
    mock_rdd = Mock()
    
    # Mock parallelize and flatMap operations
    mock_rdd.flatMap.return_value = mock_rdd
    mock_rdd.collect.return_value = [
        ["event", "2024-09-02T18:24:33Z", "12345", 1001, 2001, 3001, 4001, 5001]
    ]
    mock_sc.parallelize.return_value = mock_rdd
    
    return mock_sc

def create_sample_part_attempt():
    """Create a sample parsed part attempt."""
    return {
        'timestamp': '2024-09-02T18:24:33Z',
        'user_id': '12345',
        'section_id': 1001,
        'project_id': 2001,
        'publication_id': 3001,
        'page_id': 4001,
        'activity_id': 5001,
        'activity_revision_id': 6001,
        'attached_objectives': [101, 102],
        'page_attempt_guid': 'page-guid-123',
        'page_attempt_number': 1,
        'part_id': 'part1',
        'part_attempt_guid': 'part-guid-123',
        'part_attempt_number': 1,
        'activity_attempt_number': 1,
        'activity_attempt_guid': 'activity-guid-123',
        'score': 10,
        'out_of': 10,
        'hints': ['hint1', 'hint2'],
        'response': {'input': 'correct answer'},
        'feedback': {'content': [{'text': 'Well done!'}]},
        'activity_type': 'oli_multiple_choice',
        'session_id': '2024-09-02-12345',
        'datashop_session_id': '2024-09-02-12345'
    }