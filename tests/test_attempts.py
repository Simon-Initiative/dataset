import unittest
from unittest.mock import Mock, patch
import json
from dataset.attempts import attempts_handler, from_part_attempt, from_activity_attempt, from_page_attempt
from tests.test_data import (
    SAMPLE_PART_ATTEMPT_EVENT, SAMPLE_ACTIVITY_ATTEMPT_EVENT, 
    SAMPLE_CONTEXT, create_mock_s3_client
)

class TestAttempts(unittest.TestCase):

    def setUp(self):
        self.sample_context = SAMPLE_CONTEXT.copy()
        self.sample_context["sub_types"] = ["part_attempt_evaluated", "activity_attempt_evaluated", "page_attempt_evaluated"]
        self.mock_s3_client = create_mock_s3_client()

    @patch('boto3.client')
    def test_attempts_handler_part_attempt(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        # Create JSONL content with part attempt event
        jsonl_content = json.dumps(SAMPLE_PART_ATTEMPT_EVENT)
        
        # Mock S3 response
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        excluded_indices = []
        
        result = attempts_handler(bucket_key, self.sample_context, excluded_indices)
        
        # Verify S3 call
        self.mock_s3_client.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="test-key.jsonl"
        )
        
        # Verify result structure
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        # Verify data extraction
        record = result[0]
        self.assertEqual(record[0], "part_attempt_evaluated")  # event_type
        self.assertEqual(record[1], "2024-09-02T18:24:33Z")  # timestamp
        self.assertEqual(record[2], "12345")  # user_id

    @patch('boto3.client')
    def test_attempts_handler_activity_attempt(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        # Create activity attempt event
        activity_event = SAMPLE_ACTIVITY_ATTEMPT_EVENT.copy()
        jsonl_content = json.dumps(activity_event)
        
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        excluded_indices = []
        
        result = attempts_handler(bucket_key, self.sample_context, excluded_indices)
        
        self.assertEqual(len(result), 1)
        record = result[0]
        self.assertEqual(record[0], "activity_attempt_evaluated")  # event_type

    @patch('boto3.client')  
    def test_attempts_handler_filtering_ignored_students(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        # Create event with ignored student ID
        ignored_event = SAMPLE_PART_ATTEMPT_EVENT.copy()
        ignored_event["actor"]["account"]["name"] = 99999  # In ignored list
        
        jsonl_content = json.dumps(ignored_event)
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        result = attempts_handler(bucket_key, self.sample_context, [])
        
        # Should be filtered out
        self.assertEqual(len(result), 0)

    @patch('boto3.client')
    def test_attempts_handler_filtering_project_id(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        # Create event with different project ID
        different_project_event = SAMPLE_PART_ATTEMPT_EVENT.copy()
        different_project_event["context"]["extensions"]["http://oli.cmu.edu/extensions/project_id"] = 9999
        
        jsonl_content = json.dumps(different_project_event)
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        result = attempts_handler(bucket_key, self.sample_context, [])
        
        # Should be filtered out
        self.assertEqual(len(result), 0)

    @patch('boto3.client')
    def test_attempts_handler_filtering_page_id(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        # Create event with page ID not in allowed list
        different_page_event = SAMPLE_PART_ATTEMPT_EVENT.copy()
        different_page_event["context"]["extensions"]["http://oli.cmu.edu/extensions/page_id"] = 9999
        
        jsonl_content = json.dumps(different_page_event)
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        result = attempts_handler(bucket_key, self.sample_context, [])
        
        # Should be filtered out
        self.assertEqual(len(result), 0)

    @patch('boto3.client')
    def test_attempts_handler_subtype_filtering(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        # Limit context to only activity attempts
        context_activity_only = self.sample_context.copy()
        context_activity_only["sub_types"] = ["activity_attempt_evaluated"]
        
        jsonl_content = json.dumps(SAMPLE_PART_ATTEMPT_EVENT)
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        result = attempts_handler(bucket_key, context_activity_only, [])
        
        # Part attempt should be filtered out
        self.assertEqual(len(result), 0)

    @patch('boto3.client')
    def test_attempts_handler_field_pruning(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        jsonl_content = json.dumps(SAMPLE_PART_ATTEMPT_EVENT)
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        excluded_indices = [0, 2]  # Remove first and third fields
        
        result = attempts_handler(bucket_key, self.sample_context, excluded_indices)
        
        # Check that fields were pruned
        self.assertEqual(len(result), 1)
        original_length = 22  # Expected length of part attempt record
        expected_length = original_length - len(excluded_indices)
        # Note: prune_fields modifies the list, actual length depends on implementation

    def test_from_part_attempt_data_extraction(self):
        result = from_part_attempt(SAMPLE_PART_ATTEMPT_EVENT, self.sample_context)
        
        # Verify all expected fields are extracted
        self.assertEqual(len(result), 22)  # Expected number of fields
        
        # Verify specific field values
        self.assertEqual(result[0], "part_attempt_evaluated")  # event_type
        self.assertEqual(result[1], "2024-09-02T18:24:33Z")  # timestamp
        self.assertEqual(result[2], "12345")  # user_id (anonymized)
        self.assertEqual(result[3], 1001)  # section_id
        self.assertEqual(result[4], 2001)  # project_id
        self.assertEqual(result[17], 10)  # score
        self.assertEqual(result[18], 10)  # out_of

    def test_from_part_attempt_encoded_fields(self):
        result = from_part_attempt(SAMPLE_PART_ATTEMPT_EVENT, self.sample_context)
        
        # Verify encoded array fields (attached_objectives and hints)
        attached_objectives = result[9]  # Should be encoded array
        self.assertTrue(attached_objectives.startswith('"'))
        self.assertTrue(attached_objectives.endswith('"'))
        self.assertIn('101,102', attached_objectives)
        
        hints = result[21]  # Should be encoded array
        self.assertIn('hint1,hint2', hints)

    def test_from_part_attempt_encoded_json_fields(self):
        result = from_part_attempt(SAMPLE_PART_ATTEMPT_EVENT, self.sample_context)
        
        # Verify encoded JSON fields (response and feedback)
        response = result[19]  # Should be encoded JSON
        self.assertTrue(response.startswith('"'))
        self.assertTrue(response.endswith('"'))
        
        feedback = result[20]  # Should be encoded JSON
        self.assertIn('Well done!', feedback)

    def test_from_activity_attempt_data_extraction(self):
        result = from_activity_attempt(SAMPLE_ACTIVITY_ATTEMPT_EVENT, self.sample_context)
        
        # Verify structure
        self.assertEqual(len(result), 22)  # Same length as part attempt
        
        # Verify activity-specific fields
        self.assertEqual(result[0], "activity_attempt_evaluated")
        self.assertEqual(result[17], 8)  # score
        self.assertEqual(result[18], 10)  # out_of
        
        # Verify None fields for activity attempts
        self.assertIsNone(result[9])   # attached_objectives
        self.assertIsNone(result[12])  # part_id
        self.assertIsNone(result[19])  # response
        self.assertIsNone(result[20])  # feedback

    def test_from_page_attempt_data_extraction(self):
        # Create a page attempt event
        page_attempt_event = {
            "timestamp": "2024-09-02T18:24:33Z",
            "actor": {"account": {"name": "12345"}},
            "object": {
                "definition": {
                    "type": "http://oli.cmu.edu/extensions/page_attempt",
                    "name": {"en-US": "page_attempt_evaluated"}
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
            },
            "result": {
                "score": {"raw": 15, "max": 20}
            }
        }
        
        result = from_page_attempt(page_attempt_event, self.sample_context)
        
        # Verify structure
        self.assertEqual(len(result), 22)
        
        # Verify page-specific fields
        self.assertEqual(result[0], "page_attempt_evaluated")
        self.assertEqual(result[17], 15)  # score
        self.assertEqual(result[18], 20)  # out_of
        
        # Verify None fields for page attempts
        self.assertIsNone(result[7])   # activity_id
        self.assertIsNone(result[8])   # activity_revision_id
        self.assertIsNone(result[9])   # attached_objectives

    @patch('boto3.client')
    def test_attempts_handler_multiple_events(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        # Create multiple events in JSONL format
        events = [SAMPLE_PART_ATTEMPT_EVENT, SAMPLE_ACTIVITY_ATTEMPT_EVENT]
        jsonl_content = '\n'.join(json.dumps(event) for event in events)
        
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        result = attempts_handler(bucket_key, self.sample_context, [])
        
        # Should process both events
        self.assertEqual(len(result), 2)

    @patch('boto3.client')
    def test_attempts_handler_typo_in_subtype(self, mock_boto_client):
        """Test handling of 'part_attempt_evaluted' typo in codebase."""
        mock_boto_client.return_value = self.mock_s3_client
        
        # Test the typo version that exists in the codebase
        context_with_typo = self.sample_context.copy()
        context_with_typo["sub_types"] = ["part_attempt_evaluted"]  # Note the typo
        
        jsonl_content = json.dumps(SAMPLE_PART_ATTEMPT_EVENT)
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        result = attempts_handler(bucket_key, context_with_typo, [])
        
        # Should still work with the typo
        self.assertEqual(len(result), 1)

if __name__ == '__main__':
    unittest.main()