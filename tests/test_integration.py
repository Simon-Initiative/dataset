import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import tempfile
import os
from dataset.dataset import generate_dataset, generate_datashop
from dataset.attempts import attempts_handler
from dataset.datashop import process_jsonl_file, to_xml_message
from dataset.lookup import retrieve_lookup, post_process
from tests.test_data import (
    SAMPLE_CONTEXT, SAMPLE_LOOKUP_DATA, SAMPLE_PART_ATTEMPT_EVENT,
    create_mock_s3_client, create_sample_part_attempt
)

class TestIntegration(unittest.TestCase):
    """Integration tests that verify end-to-end workflows."""

    def setUp(self):
        self.sample_context = SAMPLE_CONTEXT.copy()
        self.sample_lookup = SAMPLE_LOOKUP_DATA.copy()
        self.mock_s3_client = create_mock_s3_client()

    @patch('dataset.dataset.build_manifests')
    @patch('dataset.dataset.save_chunk_to_s3') 
    @patch('dataset.dataset.parallel_map')
    @patch('dataset.dataset.retrieve_lookup')
    @patch('dataset.dataset.list_keys_from_inventory')
    @patch('dataset.dataset.get_event_config')
    @patch('dataset.dataset.initialize_spark_context')
    @patch('boto3.client')
    def test_end_to_end_csv_generation(self, mock_boto, mock_init_spark, mock_get_config,
                                      mock_list_keys, mock_retrieve_lookup, mock_parallel_map,
                                      mock_save_chunk, mock_build_manifests):
        """Test complete CSV dataset generation workflow."""
        
        # Setup realistic mock chain
        mock_boto.return_value = self.mock_s3_client
        mock_sc = Mock()
        mock_spark = Mock()
        mock_sc.stop = Mock()
        mock_init_spark.return_value = (mock_sc, mock_spark)
        
        # Mock event configuration with realistic handler
        from dataset.attempts import attempts_handler
        from dataset.event_registry import attempts_columns
        mock_get_config.return_value = (attempts_handler, attempts_columns)
        
        # Mock realistic key list
        mock_list_keys.return_value = [
            'section/1001/attempt_evaluated/2024/file1.jsonl',
            'section/1001/attempt_evaluated/2024/file2.jsonl'
        ]
        
        mock_retrieve_lookup.return_value = self.sample_lookup
        
        # Mock realistic data processing
        mock_parallel_map.return_value = [
            ['part_attempt_evaluated', '2024-09-02T18:24:33Z', '12345', 1001, 2001, 3001, 4001, 5001],
            ['part_attempt_evaluated', '2024-09-02T18:25:33Z', '12346', 1001, 2001, 3001, 4001, 5002]
        ]
        
        # Execute the workflow
        result = generate_dataset([1001], "attempt_evaluated", self.sample_context)
        
        # Verify the complete workflow executed
        mock_init_spark.assert_called_once()
        mock_get_config.assert_called_once_with("attempt_evaluated")
        mock_list_keys.assert_called_once()
        mock_retrieve_lookup.assert_called_once()
        mock_parallel_map.assert_called()
        mock_save_chunk.assert_called()
        mock_build_manifests.assert_called_once()
        mock_sc.stop.assert_called_once()
        
        # Verify return value
        self.assertIsInstance(result, int)
        self.assertGreater(result, 0)

    @patch('dataset.dataset.build_manifests')
    @patch('dataset.dataset.save_xml_chunk')
    @patch('dataset.dataset.process_part_attempts')
    @patch('dataset.dataset.parallel_map')
    @patch('dataset.dataset.retrieve_lookup')
    @patch('dataset.dataset.list_keys_from_inventory')
    @patch('dataset.dataset.initialize_spark_context')
    @patch('boto3.client')
    def test_end_to_end_datashop_generation(self, mock_boto, mock_init_spark, mock_list_keys,
                                           mock_retrieve_lookup, mock_parallel_map, 
                                           mock_process_attempts, mock_save_xml, mock_build_manifests):
        """Test complete Datashop XML generation workflow."""
        
        # Setup realistic mock chain
        mock_boto.return_value = self.mock_s3_client
        mock_sc = Mock()
        mock_spark = Mock()
        mock_sc.stop = Mock()
        mock_init_spark.return_value = (mock_sc, mock_spark)
        
        mock_list_keys.return_value = [
            'section/1001/attempt_evaluated/2024/file1.jsonl'
        ]
        
        mock_retrieve_lookup.return_value = self.sample_lookup
        
        # Mock realistic part attempts with proper grouping data
        mock_part_attempts = [
            {**create_sample_part_attempt(), 'user_id': '123', 'session_id': 'sess1', 'section_id': 1001},
            {**create_sample_part_attempt(), 'user_id': '123', 'session_id': 'sess1', 'section_id': 1001, 'part_attempt_number': 2},
            {**create_sample_part_attempt(), 'user_id': '456', 'session_id': 'sess2', 'section_id': 1001}
        ]
        mock_parallel_map.return_value = mock_part_attempts
        
        # Mock XML processing
        mock_process_attempts.return_value = [
            '<context_message>...</context_message>',
            '<tool_message>...</tool_message>',
            '<tutor_message>...</tutor_message>'
        ]
        
        # Execute the workflow
        result = generate_datashop(self.sample_context)
        
        # Verify datashop-specific workflow
        mock_list_keys.assert_called_with(
            self.sample_context["section_ids"], 
            "attempt_evaluated",
            self.sample_context["bucket_name"],
            self.sample_context["inventory_bucket_name"]
        )
        
        # Verify grouping/sorting occurred by checking process_part_attempts calls
        self.assertTrue(mock_process_attempts.called)
        
        # Should group by section_id + user_id + session_id, so 2 groups expected
        call_count = mock_process_attempts.call_count
        self.assertEqual(call_count, 2)  # Two unique user sessions
        
        mock_save_xml.assert_called()
        mock_build_manifests.assert_called_once()

    @patch('boto3.client')
    def test_attempts_handler_integration_with_real_data(self, mock_boto):
        """Test attempts handler with realistic JSON data structures."""
        
        mock_s3_client = Mock()
        mock_boto.return_value = mock_s3_client
        
        # Create realistic multi-line JSONL with various event types
        events = [
            # Valid part attempt
            {
                **SAMPLE_PART_ATTEMPT_EVENT,
                "result": {
                    "score": {"raw": 8, "max": 10},
                    "response": {"input": "student answer"},
                    "extensions": {
                        "http://oli.cmu.edu/extensions/feedback": {
                            "content": [{"text": "Good try, but not quite right."}]
                        }
                    }
                }
            },
            # Different student
            {
                **SAMPLE_PART_ATTEMPT_EVENT,
                "actor": {"account": {"name": "67890"}},
                "result": {
                    "score": {"raw": 10, "max": 10},
                    "response": {"input": "correct answer"},
                    "extensions": {
                        "http://oli.cmu.edu/extensions/feedback": {
                            "content": [{"text": "Excellent work!"}]
                        }
                    }
                }
            }
        ]
        
        jsonl_content = '\n'.join(json.dumps(event) for event in events)
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        mock_s3_client.get_object.return_value = mock_response
        
        context = self.sample_context.copy()
        context["sub_types"] = ["part_attempt_evaluated"]
        
        bucket_key = ("test-bucket", "realistic-data.jsonl")
        result = attempts_handler(bucket_key, context, [])
        
        # Verify both events were processed
        self.assertEqual(len(result), 2)
        
        # Verify data extraction for first event
        record1 = result[0]
        self.assertEqual(record1[17], 8)  # score
        self.assertEqual(record1[18], 10)  # out_of
        self.assertIn("student answer", record1[19])  # response
        
        # Verify data extraction for second event  
        record2 = result[1]
        self.assertEqual(record2[2], "67890")  # user_id
        self.assertEqual(record2[17], 10)  # score

    @patch('boto3.client')
    def test_datashop_processing_integration_with_hints(self, mock_boto):
        """Test datashop processing with realistic hint data."""
        
        mock_s3_client = Mock()
        mock_boto.return_value = mock_s3_client
        
        # Create event with hints requested
        event_with_hints = SAMPLE_PART_ATTEMPT_EVENT.copy()
        event_with_hints["context"]["extensions"]["http://oli.cmu.edu/extensions/hints_requested"] = ["hint1", "hint2"]
        
        jsonl_content = json.dumps(event_with_hints)
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        mock_s3_client.get_object.return_value = mock_response
        
        # Setup context with hint definitions
        context = self.sample_context.copy()
        lookup_with_hints = self.sample_lookup.copy()
        lookup_with_hints['activities']['5001']['parts']['part1'] = {
            'hints': [
                {'id': 'hint1', 'content': [{'text': 'First helpful hint'}]},
                {'id': 'hint2', 'content': [{'text': 'Second helpful hint'}]}
            ]
        }
        context['lookup'] = lookup_with_hints
        
        bucket_key = ("test-bucket", "hints-data.jsonl")
        result = process_jsonl_file(bucket_key, context, [])
        
        # Verify part attempt was created with hints
        self.assertEqual(len(result), 1)
        part_attempt = result[0]
        self.assertEqual(part_attempt['hints'], ['hint1', 'hint2'])
        
        # Test XML generation with hints
        xml_result = to_xml_message(part_attempt, lookup_with_hints)
        
        # Verify hint messages are generated
        self.assertIn('<tool_message', xml_result)
        self.assertIn('<tutor_message', xml_result)
        self.assertIn('HINT_REQUEST', xml_result)
        self.assertIn('First helpful hint', xml_result)
        self.assertIn('Second helpful hint', xml_result)

    def test_lookup_processing_integration(self):
        """Test complete lookup data processing chain."""
        
        # Start with raw lookup data
        raw_lookup = {
            'activities': {
                'activity1': {
                    'parts': [  # Array format
                        {
                            'id': 'part1',
                            'hints': [
                                {'id': 'h1', 'content': [{'text': 'Hint 1'}]}
                            ]
                        },
                        {
                            'id': 'part2', 
                            'hints': []
                        }
                    ]
                }
            },
            'hierarchy': {
                '1': {'title': 'Course', 'children': [2, 3]},
                '2': {'title': 'Unit 1', 'children': [4]},
                '3': {'title': 'Unit 2', 'children': [5]}, 
                '4': {'title': 'Page 1', 'children': []},
                '5': {'title': 'Page 2', 'children': []}
            }
        }
        
        # Process through post_process
        processed = post_process(raw_lookup)
        
        # Verify parts were converted to dict
        activity = processed['activities']['activity1']
        self.assertIsInstance(activity['parts'], dict)
        self.assertIn('part1', activity['parts'])
        self.assertIn('part2', activity['parts'])
        
        # Verify hierarchy ancestors were calculated
        self.assertEqual(processed['hierarchy']['2']['ancestors'], [1])
        self.assertEqual(processed['hierarchy']['4']['ancestors'], [1, 2])
        self.assertEqual(processed['hierarchy']['5']['ancestors'], [1, 3])

    @patch('dataset.dataset.list_keys_from_inventory')
    @patch('dataset.dataset.initialize_spark_context')
    @patch('boto3.client')
    def test_error_recovery_in_chunked_processing(self, mock_boto, mock_init_spark, mock_list_keys):
        """Test that the system gracefully handles errors in individual chunks."""
        
        mock_boto.return_value = self.mock_s3_client
        mock_sc = Mock()
        mock_spark = Mock()
        mock_sc.stop = Mock()
        mock_init_spark.return_value = (mock_sc, mock_spark)
        
        # Return enough keys to create multiple chunks
        mock_list_keys.return_value = ['key1', 'key2', 'key3', 'key4', 'key5']
        
        with patch('dataset.dataset.get_event_config') as mock_get_config, \
             patch('dataset.dataset.retrieve_lookup') as mock_retrieve_lookup, \
             patch('dataset.dataset.parallel_map') as mock_parallel_map, \
             patch('dataset.dataset.save_chunk_to_s3') as mock_save_chunk, \
             patch('dataset.dataset.build_manifests') as mock_build_manifests, \
             patch('builtins.print') as mock_print:
            
            mock_get_config.return_value = (Mock(), ['col1'])
            mock_retrieve_lookup.return_value = {}
            
            # Make parallel_map fail on first chunk, succeed on others
            mock_parallel_map.side_effect = [
                Exception("Chunk processing error"),
                [['data2']],
                [['data3']]
            ]
            
            # Set small chunk size to ensure multiple chunks
            context = self.sample_context.copy()
            context['chunk_size'] = 2
            
            result = generate_dataset([1001], "attempt_evaluated", context)
            
            # Should complete despite error in first chunk
            self.assertIsInstance(result, int)
            
            # Should have attempted to process all chunks
            self.assertEqual(mock_parallel_map.call_count, 3)
            
            # Should have saved successful chunks
            self.assertEqual(mock_save_chunk.call_count, 2)
            
            # Should have printed error message
            error_calls = [call for call in mock_print.call_args_list 
                          if 'Error processing chunk' in str(call)]
            self.assertEqual(len(error_calls), 1)

    def test_data_consistency_across_processing_stages(self):
        """Test that data maintains consistency as it flows through processing stages."""
        
        # Start with a realistic event
        original_event = {
            "timestamp": "2024-09-02T18:24:33Z",
            "actor": {"account": {"name": "test-student-123"}},
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
                    "http://oli.cmu.edu/extensions/attached_objectives": [101, 102, 103],
                    "http://oli.cmu.edu/extensions/page_attempt_guid": "page-guid-unique",
                    "http://oli.cmu.edu/extensions/page_attempt_number": 1,
                    "http://oli.cmu.edu/extensions/part_id": "complex-part-id",
                    "http://oli.cmu.edu/extensions/part_attempt_guid": "part-guid-unique", 
                    "http://oli.cmu.edu/extensions/part_attempt_number": 3,
                    "http://oli.cmu.edu/extensions/activity_attempt_number": 2,
                    "http://oli.cmu.edu/extensions/activity_attempt_guid": "activity-guid-unique",
                    "http://oli.cmu.edu/extensions/hints_requested": ["h1", "h2", "h3"]
                }
            },
            "result": {
                "score": {"raw": 7, "max": 10},
                "response": {"input": "Complex student response with special chars: <>&\""},
                "extensions": {
                    "http://oli.cmu.edu/extensions/feedback": {
                        "content": [
                            {"text": "Feedback part 1. "},
                            {
                                "text": "Feedback part 2 with ",
                                "children": [{"text": "nested content"}]
                            }
                        ]
                    }
                }
            }
        }
        
        context = {'anonymize': False, 'users': {}}
        
        # Stage 1: Parse attempt
        from dataset.datashop import parse_attempt
        parsed = parse_attempt(original_event, context)
        
        # Verify key data preservation
        self.assertEqual(parsed['timestamp'], "2024-09-02T18:24:33Z")
        self.assertEqual(parsed['user_id'], "test-student-123")
        self.assertEqual(parsed['section_id'], 1001)
        self.assertEqual(parsed['activity_id'], 5001)
        self.assertEqual(parsed['part_attempt_number'], 3)
        self.assertEqual(parsed['score'], 7)
        self.assertEqual(parsed['hints'], ["h1", "h2", "h3"])
        
        # Stage 2: CSV processing (from attempts handler)
        from dataset.attempts import from_part_attempt
        csv_record = from_part_attempt(original_event, self.sample_context)
        
        # Verify consistency with parsed data
        self.assertEqual(csv_record[1], parsed['timestamp'])  # timestamp
        self.assertEqual(csv_record[3], parsed['section_id'])  # section_id
        self.assertEqual(csv_record[7], parsed['activity_id'])  # activity_id
        self.assertEqual(csv_record[17], parsed['score'])  # score
        
        # Stage 3: Datashop XML processing
        from dataset.datashop import to_xml_message
        xml_context = self.sample_lookup.copy()
        xml_result = to_xml_message(parsed, xml_context)
        
        # Verify XML contains key data (basic checks)
        self.assertIn('test-student-123', xml_result)
        self.assertIn('2024-09-02 18:24:33', xml_result)  # Formatted timestamp
        self.assertIn('Activity 5001', xml_result)
        
        # Verify special character handling
        self.assertIn('Complex student response', xml_result)
        # XML should be properly escaped or encoded
        self.assertTrue('&lt;' in xml_result or '&#x' in xml_result or '<' not in xml_result.split('>')[1])

if __name__ == '__main__':
    unittest.main()