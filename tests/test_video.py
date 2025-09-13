import unittest
from unittest.mock import Mock, patch
import json
from dataset.video import video_handler, from_played, from_paused, from_seeked, from_completed
from tests.test_data import SAMPLE_VIDEO_PLAYED_EVENT, SAMPLE_CONTEXT, create_mock_s3_client

class TestVideo(unittest.TestCase):

    def setUp(self):
        self.sample_context = SAMPLE_CONTEXT.copy()
        self.sample_context["sub_types"] = ["played", "paused", "seeked", "completed"]
        self.mock_s3_client = create_mock_s3_client()

    def create_video_event(self, verb, additional_extensions=None):
        """Helper to create video events with different verbs."""
        event = SAMPLE_VIDEO_PLAYED_EVENT.copy()
        event["verb"]["display"]["en-US"] = verb
        
        if additional_extensions:
            event["result"]["extensions"].update(additional_extensions)
        
        return event

    @patch('boto3.client')
    def test_video_handler_played_event(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        # Create JSONL content with video played event
        jsonl_content = json.dumps(SAMPLE_VIDEO_PLAYED_EVENT)
        
        # Mock S3 response
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        excluded_indices = []
        
        result = video_handler(bucket_key, self.sample_context, excluded_indices)
        
        # Verify S3 call
        self.mock_s3_client.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="test-key.jsonl"
        )
        
        # Verify result structure
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        # Verify data extraction
        record = result[0]
        self.assertEqual(record[0], "played")  # event_type
        self.assertEqual(record[1], "2024-09-02T18:30:33Z")  # timestamp
        self.assertEqual(record[2], "12345")  # user_id

    @patch('boto3.client')
    def test_video_handler_paused_event(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        paused_event = self.create_video_event("paused", {
            "https://w3id.org/xapi/video/extensions/played-segments": "0[.]45",
            "https://w3id.org/xapi/video/extensions/progress": 0.15
        })
        
        jsonl_content = json.dumps(paused_event)
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        result = video_handler(bucket_key, self.sample_context, [])
        
        self.assertEqual(len(result), 1)
        record = result[0]
        self.assertEqual(record[0], "paused")

    @patch('boto3.client')
    def test_video_handler_seeked_event(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        seeked_event = self.create_video_event("seeked", {
            "https://w3id.org/xapi/video/extensions/time-from": 30,
            "https://w3id.org/xapi/video/extensions/time-to": 60
        })
        
        jsonl_content = json.dumps(seeked_event)
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        result = video_handler(bucket_key, self.sample_context, [])
        
        self.assertEqual(len(result), 1)
        record = result[0]
        self.assertEqual(record[0], "seeked")

    @patch('boto3.client')
    def test_video_handler_completed_event(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        completed_event = self.create_video_event("completed", {
            "https://w3id.org/xapi/video/extensions/played-segments": "0[.]300",
            "https://w3id.org/xapi/video/extensions/progress": 1.0
        })
        
        jsonl_content = json.dumps(completed_event)
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        result = video_handler(bucket_key, self.sample_context, [])
        
        self.assertEqual(len(result), 1)
        record = result[0]
        self.assertEqual(record[0], "completed")

    @patch('boto3.client')
    def test_video_handler_filtering_ignored_students(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        # Create event with ignored student ID
        ignored_event = SAMPLE_VIDEO_PLAYED_EVENT.copy()
        ignored_event["actor"]["account"]["name"] = 99999  # In ignored list
        
        jsonl_content = json.dumps(ignored_event)
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        result = video_handler(bucket_key, self.sample_context, [])
        
        # Should be filtered out
        self.assertEqual(len(result), 0)

    @patch('boto3.client')
    def test_video_handler_filtering_project_id(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        # Create event with different project ID
        different_project_event = SAMPLE_VIDEO_PLAYED_EVENT.copy()
        different_project_event["context"]["extensions"]["http://oli.cmu.edu/extensions/project_id"] = 9999
        
        jsonl_content = json.dumps(different_project_event)
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        result = video_handler(bucket_key, self.sample_context, [])
        
        # Should be filtered out
        self.assertEqual(len(result), 0)

    @patch('boto3.client')
    def test_video_handler_filtering_page_id(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        # Create event with resource_id not in allowed page list
        different_page_event = SAMPLE_VIDEO_PLAYED_EVENT.copy()
        different_page_event["context"]["extensions"]["http://oli.cmu.edu/extensions/resource_id"] = 9999
        
        jsonl_content = json.dumps(different_page_event)
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        result = video_handler(bucket_key, self.sample_context, [])
        
        # Should be filtered out
        self.assertEqual(len(result), 0)

    @patch('boto3.client')
    def test_video_handler_subtype_filtering(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        # Limit context to only paused events
        context_paused_only = self.sample_context.copy()
        context_paused_only["sub_types"] = ["paused"]
        
        jsonl_content = json.dumps(SAMPLE_VIDEO_PLAYED_EVENT)  # played event
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        result = video_handler(bucket_key, context_paused_only, [])
        
        # Played event should be filtered out
        self.assertEqual(len(result), 0)

    def test_from_played_data_extraction(self):
        result = from_played(SAMPLE_VIDEO_PLAYED_EVENT, self.sample_context)
        
        # Verify all expected fields are extracted
        self.assertEqual(len(result), 17)  # Expected number of fields for video events
        
        # Verify specific field values
        self.assertEqual(result[0], "played")  # event_type
        self.assertEqual(result[1], "2024-09-02T18:30:33Z")  # timestamp
        self.assertEqual(result[2], "12345")  # user_id
        self.assertEqual(result[10], "https://example.com/video123")  # video_url
        self.assertEqual(result[11], "Sample Video")  # video_title
        self.assertEqual(result[12], 300)  # video_length
        self.assertEqual(result[13], 45)  # video_time_from
        self.assertIsNone(result[14])  # video_time_to (None for played)
        self.assertIsNone(result[15])  # video_played_segments (None for played)
        self.assertIsNone(result[16])  # video_progress (None for played)

    def test_from_paused_data_extraction(self):
        paused_event = self.create_video_event("paused", {
            "https://w3id.org/xapi/video/extensions/played-segments": "0[.]45",
            "https://w3id.org/xapi/video/extensions/progress": 0.15
        })
        
        result = from_paused(paused_event, self.sample_context)
        
        # Verify paused-specific fields
        self.assertEqual(result[0], "paused")
        self.assertEqual(result[13], 45)  # video_time_from
        self.assertIsNone(result[14])  # video_time_to (None for paused)
        self.assertEqual(result[15], "0[.]45")  # video_played_segments
        self.assertEqual(result[16], 0.15)  # video_progress

    def test_from_seeked_data_extraction(self):
        seeked_event = self.create_video_event("seeked", {
            "https://w3id.org/xapi/video/extensions/time-from": 30,
            "https://w3id.org/xapi/video/extensions/time-to": 60
        })
        
        result = from_seeked(seeked_event, self.sample_context)
        
        # Verify seeked-specific fields
        self.assertEqual(result[0], "seeked")
        self.assertIsNone(result[12])  # video_length (None for seeked)
        self.assertEqual(result[13], 60)  # video_time_to
        self.assertEqual(result[14], 30)  # video_time_from
        self.assertIsNone(result[15])  # video_played_segments (None for seeked)
        self.assertIsNone(result[16])  # video_progress (None for seeked)

    def test_from_completed_data_extraction(self):
        completed_event = self.create_video_event("completed", {
            "https://w3id.org/xapi/video/extensions/played-segments": "0[.]300",
            "https://w3id.org/xapi/video/extensions/progress": 1.0
        })
        
        result = from_completed(completed_event, self.sample_context)
        
        # Verify completed-specific fields
        self.assertEqual(result[0], "completed")
        self.assertEqual(result[12], 300)  # video_length
        self.assertEqual(result[13], 45)  # video_time_from
        self.assertIsNone(result[14])  # video_time_to (None for completed)
        self.assertEqual(result[15], "0[.]300")  # video_played_segments
        self.assertEqual(result[16], 1.0)  # video_progress

    @patch('boto3.client')
    def test_video_handler_multiple_event_types(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        # Create multiple different video events
        played_event = self.create_video_event("played")
        paused_event = self.create_video_event("paused")
        seeked_event = self.create_video_event("seeked")
        
        events = [played_event, paused_event, seeked_event]
        jsonl_content = '\n'.join(json.dumps(event) for event in events)
        
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        result = video_handler(bucket_key, self.sample_context, [])
        
        # Should process all three events
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0][0], "played")
        self.assertEqual(result[1][0], "paused")
        self.assertEqual(result[2][0], "seeked")

    @patch('boto3.client')
    def test_video_handler_unknown_verb(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        # Create event with unknown verb
        unknown_event = self.create_video_event("unknown_verb")
        
        jsonl_content = json.dumps(unknown_event)
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        result = video_handler(bucket_key, self.sample_context, [])
        
        # Should be filtered out (unknown verb not in sub_types)
        self.assertEqual(len(result), 0)

    @patch('boto3.client')
    def test_video_handler_field_pruning(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        jsonl_content = json.dumps(SAMPLE_VIDEO_PLAYED_EVENT)
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        excluded_indices = [0, 5]  # Remove some fields
        
        result = video_handler(bucket_key, self.sample_context, excluded_indices)
        
        # Check that processing succeeds with field pruning
        self.assertEqual(len(result), 1)

if __name__ == '__main__':
    unittest.main()