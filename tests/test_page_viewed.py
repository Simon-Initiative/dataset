import unittest
from unittest.mock import Mock, patch
import json
from dataset.page_viewed import page_viewed_handler, from_page_viewed
from tests.test_data import SAMPLE_PAGE_VIEWED_EVENT, SAMPLE_CONTEXT, create_mock_s3_client

class TestPageViewed(unittest.TestCase):

    def setUp(self):
        self.sample_context = SAMPLE_CONTEXT.copy()
        self.mock_s3_client = create_mock_s3_client()

    @patch('boto3.client')
    def test_page_viewed_handler_success(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        # Create JSONL content with page viewed event
        jsonl_content = json.dumps(SAMPLE_PAGE_VIEWED_EVENT)
        
        # Mock S3 response
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        excluded_indices = []
        
        result = page_viewed_handler(bucket_key, self.sample_context, excluded_indices)
        
        # Verify S3 call
        self.mock_s3_client.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="test-key.jsonl"
        )
        
        # Verify result structure
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        # Verify data extraction
        record = result[0]
        self.assertEqual(record[0], "page_viewed")  # event_type
        self.assertEqual(record[1], "2024-09-02T18:20:33Z")  # timestamp
        self.assertEqual(record[2], "12345")  # user_id

    @patch('boto3.client')
    def test_page_viewed_handler_filtering_ignored_students(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        # Create event with ignored student ID
        ignored_event = SAMPLE_PAGE_VIEWED_EVENT.copy()
        ignored_event["actor"]["account"]["name"] = 99999  # In ignored list
        
        jsonl_content = json.dumps(ignored_event)
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        result = page_viewed_handler(bucket_key, self.sample_context, [])
        
        # Should be filtered out
        self.assertEqual(len(result), 0)

    @patch('boto3.client')
    def test_page_viewed_handler_filtering_project_id(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        # Create event with different project ID
        different_project_event = SAMPLE_PAGE_VIEWED_EVENT.copy()
        different_project_event["context"]["extensions"]["http://oli.cmu.edu/extensions/project_id"] = 9999
        
        jsonl_content = json.dumps(different_project_event)
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        result = page_viewed_handler(bucket_key, self.sample_context, [])
        
        # Should be filtered out
        self.assertEqual(len(result), 0)

    @patch('boto3.client')
    def test_page_viewed_handler_filtering_page_id(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        # Create event with page ID not in allowed list
        different_page_event = SAMPLE_PAGE_VIEWED_EVENT.copy()
        different_page_event["context"]["extensions"]["http://oli.cmu.edu/extensions/page_id"] = 9999
        
        jsonl_content = json.dumps(different_page_event)
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        result = page_viewed_handler(bucket_key, self.sample_context, [])
        
        # Should be filtered out
        self.assertEqual(len(result), 0)

    @patch('boto3.client')
    def test_page_viewed_handler_project_id_none_allows_all(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        # Set project_id to None to allow all projects
        context_no_project_filter = self.sample_context.copy()
        context_no_project_filter["project_id"] = None
        
        jsonl_content = json.dumps(SAMPLE_PAGE_VIEWED_EVENT)
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        result = page_viewed_handler(bucket_key, context_no_project_filter, [])
        
        # Should not be filtered out
        self.assertEqual(len(result), 1)

    @patch('boto3.client')
    def test_page_viewed_handler_page_ids_none_allows_all(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        # Set page_ids to None to allow all pages
        context_no_page_filter = self.sample_context.copy()
        context_no_page_filter["page_ids"] = None
        
        jsonl_content = json.dumps(SAMPLE_PAGE_VIEWED_EVENT)
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        result = page_viewed_handler(bucket_key, context_no_page_filter, [])
        
        # Should not be filtered out
        self.assertEqual(len(result), 1)

    @patch('boto3.client')
    def test_page_viewed_handler_field_pruning(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        jsonl_content = json.dumps(SAMPLE_PAGE_VIEWED_EVENT)
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        excluded_indices = [0, 2]  # Remove first and third fields
        
        result = page_viewed_handler(bucket_key, self.sample_context, excluded_indices)
        
        # Check that processing succeeds with field pruning
        self.assertEqual(len(result), 1)

    def test_from_page_viewed_data_extraction(self):
        result = from_page_viewed(SAMPLE_PAGE_VIEWED_EVENT, self.sample_context)
        
        # Verify all expected fields are extracted
        self.assertEqual(len(result), 9)  # Expected number of fields for page_viewed
        
        # Verify specific field values
        self.assertEqual(result[0], "page_viewed")  # event_type
        self.assertEqual(result[1], "2024-09-02T18:20:33Z")  # timestamp
        self.assertEqual(result[2], "12345")  # user_id (anonymized)
        self.assertEqual(result[3], 1001)  # section_id
        self.assertEqual(result[4], 2001)  # project_id
        self.assertEqual(result[5], 3001)  # publication_id
        self.assertEqual(result[6], 4001)  # page_id
        self.assertEqual(result[7], "page-guid-123")  # page_attempt_guid
        self.assertEqual(result[8], 1)  # page_attempt_number

    def test_from_page_viewed_with_anonymized_context(self):
        # Test with anonymized context
        anonymized_context = self.sample_context.copy()
        anonymized_context["anonymize"] = True
        
        result = from_page_viewed(SAMPLE_PAGE_VIEWED_EVENT, anonymized_context)
        
        # User ID should remain as original value when anonymized
        self.assertEqual(result[2], "12345")

    def test_from_page_viewed_with_non_anonymized_context(self):
        # Test with non-anonymized context and lookup data
        non_anonymized_context = self.sample_context.copy()
        non_anonymized_context["anonymize"] = False
        non_anonymized_context["lookup"] = {
            "users": {
                "12345": {"email": "test@example.com"}
            }
        }
        
        result = from_page_viewed(SAMPLE_PAGE_VIEWED_EVENT, non_anonymized_context)
        
        # User ID should be replaced with email when not anonymized
        self.assertEqual(result[2], "test@example.com")

    @patch('boto3.client')
    def test_page_viewed_handler_multiple_events(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        # Create multiple page viewed events
        event1 = SAMPLE_PAGE_VIEWED_EVENT.copy()
        event2 = SAMPLE_PAGE_VIEWED_EVENT.copy()
        event2["actor"]["account"]["name"] = "67890"
        event2["timestamp"] = "2024-09-02T18:25:33Z"
        
        events = [event1, event2]
        jsonl_content = '\n'.join(json.dumps(event) for event in events)
        
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        result = page_viewed_handler(bucket_key, self.sample_context, [])
        
        # Should process both events
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0][2], "12345")  # First user
        self.assertEqual(result[1][2], "67890")  # Second user

    @patch('boto3.client')
    def test_page_viewed_handler_empty_file(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        # Mock empty file
        mock_response = Mock()
        mock_response['Body'].read.return_value = b''
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "empty-file.jsonl")
        result = page_viewed_handler(bucket_key, self.sample_context, [])
        
        # Should return empty list
        self.assertEqual(result, [])

    @patch('boto3.client')
    def test_page_viewed_handler_malformed_json(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        # Mock file with malformed JSON
        mock_response = Mock()
        mock_response['Body'].read.return_value = b'{"invalid": json}'
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "malformed.jsonl")
        
        # Should raise JSON decode error
        with self.assertRaises(json.JSONDecodeError):
            page_viewed_handler(bucket_key, self.sample_context, [])

if __name__ == '__main__':
    unittest.main()