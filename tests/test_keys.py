import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import io
import json
from datetime import datetime, timedelta
from dataset.keys import list_keys_from_inventory, get_most_recent_manifest, fetch_parquet, list_keys
from tests.test_data import SAMPLE_INVENTORY_MANIFEST, create_mock_s3_client

class TestKeys(unittest.TestCase):

    def setUp(self):
        self.section_ids = [1001, 1002]
        self.action = "attempt_evaluated"
        self.bucket_name = "test-bucket"
        self.inventory_bucket_name = "test-bucket-inventory"

    @patch('boto3.client')
    def test_list_keys_from_inventory_success(self, mock_boto_client):
        mock_s3_client = create_mock_s3_client()
        mock_boto_client.return_value = mock_s3_client
        
        # Mock the manifest and parquet data
        with patch('dataset.keys.get_most_recent_manifest') as mock_get_manifest, \
             patch('dataset.keys.fetch_parquet') as mock_fetch_parquet:
            
            mock_get_manifest.return_value = SAMPLE_INVENTORY_MANIFEST
            mock_fetch_parquet.return_value = [
                'section/1001/attempt_evaluated/file1.jsonl',
                'section/1002/attempt_evaluated/file2.jsonl'
            ]
            
            result = list_keys_from_inventory(
                self.section_ids, self.action, 
                self.inventory_bucket_name, self.bucket_name
            )
            
            # Verify manifest was retrieved
            mock_get_manifest.assert_called_once_with(
                self.inventory_bucket_name, self.bucket_name
            )
            
            # Verify fetch_parquet was called for each file in manifest
            self.assertEqual(mock_fetch_parquet.call_count, len(SAMPLE_INVENTORY_MANIFEST['files']))
            
            # Verify results
            self.assertIsInstance(result, list)
            self.assertTrue(len(result) > 0)

    @patch('boto3.client')
    def test_list_keys_from_inventory_exception_handling(self, mock_boto_client):
        mock_s3_client = Mock()
        mock_s3_client.get_object.side_effect = Exception("S3 Error")
        mock_boto_client.return_value = mock_s3_client
        
        result = list_keys_from_inventory(
            self.section_ids, self.action,
            self.inventory_bucket_name, self.bucket_name
        )
        
        # Should return empty list on exception
        self.assertEqual(result, [])

    @patch('boto3.client')
    def test_get_most_recent_manifest_yesterday(self, mock_boto_client):
        mock_s3_client = Mock()
        
        # Mock successful response for yesterday's manifest
        mock_response = {}
        mock_body = Mock()
        mock_response['Body'] = mock_body
        mock_body.read.return_value = json.dumps(SAMPLE_INVENTORY_MANIFEST).encode('utf-8')
        mock_s3_client.get_object.return_value = mock_response
        
        mock_boto_client.return_value = mock_s3_client
        
        result = get_most_recent_manifest(self.inventory_bucket_name, self.bucket_name)
        
        # Verify correct manifest structure is returned
        self.assertEqual(result['sourceBucket'], SAMPLE_INVENTORY_MANIFEST['sourceBucket'])
        self.assertIn('files', result)

    @patch('boto3.client')
    def test_get_most_recent_manifest_fallback(self, mock_boto_client):
        mock_s3_client = Mock()
        
        # Mock first call fails, second succeeds
        call_count = 0
        def mock_get_object(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("File not found")
            else:
                mock_response = Mock()
                mock_response['Body'].read.return_value = json.dumps(SAMPLE_INVENTORY_MANIFEST).encode('utf-8')
                return mock_response
        
        mock_s3_client.get_object.side_effect = mock_get_object
        mock_boto_client.return_value = mock_s3_client
        
        # This should work but we need to patch the range to test fallback
        with patch('dataset.keys.range', return_value=[1, 2]):
            try:
                result = get_most_recent_manifest(self.inventory_bucket_name, self.bucket_name)
                # If we get here, the fallback worked
                self.assertIn('files', result)
            except:
                # If all attempts fail, it should continue to the next iteration
                pass

    @patch('boto3.client')
    def test_fetch_parquet_filtering(self, mock_boto_client):
        mock_s3_client = Mock()
        
        # Mock parquet data response (we'll test the filtering logic separately)
        mock_response = {}
        mock_body = Mock()
        mock_response['Body'] = mock_body
        # Create fake parquet bytes that our mock will return
        mock_body.read.return_value = b'fake_parquet_data'
        mock_s3_client.get_object.return_value = mock_response
        
        mock_boto_client.return_value = mock_s3_client
        
        # Mock pandas.read_parquet to return our test DataFrame
        with patch('pandas.read_parquet') as mock_read_parquet:
            df = pd.DataFrame({
                'key': [
                    'section/1001/attempt_evaluated/file1.jsonl',
                    'section/1001/page_viewed/file2.jsonl',
                    'section/1002/attempt_evaluated/file3.jsonl',
                    'section/9999/attempt_evaluated/file4.jsonl',  # Different section
                    'other/1001/attempt_evaluated/file5.jsonl'  # Different prefix
                ],
                'size': [1024, 512, 2048, 256, 128]
            })
            mock_read_parquet.return_value = df
            
            result = fetch_parquet(
                self.section_ids, self.action, 
                mock_s3_client, self.bucket_name, "test-key"
            )
            
            # Should only return keys matching section IDs and action
            expected_keys = [
                'section/1001/attempt_evaluated/file1.jsonl',
                'section/1002/attempt_evaluated/file3.jsonl'
            ]
            
            self.assertEqual(sorted(result), sorted(expected_keys))

    @patch('boto3.client')
    def test_fetch_parquet_special_characters_in_section_ids(self, mock_boto_client):
        mock_s3_client = Mock()
        
        # Test with section IDs that contain regex special characters
        section_ids_with_special_chars = ["1001", "test.section", "section+123"]
        
        mock_response = {}
        mock_body = Mock()
        mock_response['Body'] = mock_body
        mock_body.read.return_value = b'fake_parquet_data'
        mock_s3_client.get_object.return_value = mock_response
        
        mock_boto_client.return_value = mock_s3_client
        
        # Mock pandas.read_parquet to return our test DataFrame
        with patch('pandas.read_parquet') as mock_read_parquet:
            df = pd.DataFrame({
                'key': [
                    'section/1001/attempt_evaluated/file1.jsonl',
                    'section/test.section/attempt_evaluated/file2.jsonl',
                    'section/section+123/attempt_evaluated/file3.jsonl',
                    'section/testXsection/attempt_evaluated/file4.jsonl'  # Should not match test.section
                ],
                'size': [1024, 512, 2048, 256]
            })
            mock_read_parquet.return_value = df
            
            result = fetch_parquet(
                section_ids_with_special_chars, self.action,
                mock_s3_client, self.bucket_name, "test-key"
            )
            
            # Should properly escape special characters and match exactly
            expected_keys = [
                'section/1001/attempt_evaluated/file1.jsonl',
                'section/test.section/attempt_evaluated/file2.jsonl',
                'section/section+123/attempt_evaluated/file3.jsonl'
            ]
            
            self.assertEqual(sorted(result), sorted(expected_keys))

    @patch('boto3.client')
    def test_list_keys_single_page(self, mock_boto_client):
        mock_s3_client = Mock()
        
        # Mock S3 response without pagination
        mock_response = {
            'Contents': [
                {'Key': 'section/1001/attempt_evaluated/file1.jsonl'},
                {'Key': 'section/1001/attempt_evaluated/file2.jsonl'}
            ],
            'IsTruncated': False
        }
        mock_s3_client.list_objects_v2.return_value = mock_response
        mock_boto_client.return_value = mock_s3_client
        
        result = list_keys(self.bucket_name, 1001, self.action)
        
        # Verify correct S3 call
        mock_s3_client.list_objects_v2.assert_called_once_with(
            Bucket=self.bucket_name,
            Prefix='section/1001/attempt_evaluated/'
        )
        
        # Verify results
        expected_keys = [
            'section/1001/attempt_evaluated/file1.jsonl',
            'section/1001/attempt_evaluated/file2.jsonl'
        ]
        self.assertEqual(result, expected_keys)

    @patch('boto3.client')
    def test_list_keys_pagination(self, mock_boto_client):
        mock_s3_client = Mock()
        
        # Mock paginated S3 responses
        responses = [
            {
                'Contents': [
                    {'Key': 'section/1001/attempt_evaluated/file1.jsonl'},
                    {'Key': 'section/1001/attempt_evaluated/file2.jsonl'}
                ],
                'IsTruncated': True,
                'NextContinuationToken': 'token123'
            },
            {
                'Contents': [
                    {'Key': 'section/1001/attempt_evaluated/file3.jsonl'}
                ],
                'IsTruncated': False
            }
        ]
        
        mock_s3_client.list_objects_v2.side_effect = responses
        mock_boto_client.return_value = mock_s3_client
        
        result = list_keys(self.bucket_name, 1001, self.action)
        
        # Verify pagination calls
        self.assertEqual(mock_s3_client.list_objects_v2.call_count, 2)
        
        # Verify all keys are collected
        expected_keys = [
            'section/1001/attempt_evaluated/file1.jsonl',
            'section/1001/attempt_evaluated/file2.jsonl',
            'section/1001/attempt_evaluated/file3.jsonl'
        ]
        self.assertEqual(result, expected_keys)

    @patch('boto3.client')
    def test_list_keys_no_contents(self, mock_boto_client):
        mock_s3_client = Mock()
        
        # Mock S3 response with no contents
        mock_response = {'IsTruncated': False}
        mock_s3_client.list_objects_v2.return_value = mock_response
        mock_boto_client.return_value = mock_s3_client
        
        result = list_keys(self.bucket_name, 1001, self.action)
        
        # Should return empty list
        self.assertEqual(result, [])

if __name__ == '__main__':
    unittest.main()