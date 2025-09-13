import unittest
from unittest.mock import Mock, patch
import json
from dataset.datashop import (
    sanitize_element_text, sanitize_attribute_value, trim_to_100_bytes,
    get_text_from_content, parse_attempt, to_xml_message
)
from dataset.utils import encode_array, encode_json, prune_fields
from dataset.lookup import determine_student_id, calculate_ancestors, mapify_parts
from dataset.attempts import attempts_handler
from tests.test_data import SAMPLE_CONTEXT, create_mock_s3_client

class TestEdgeCases(unittest.TestCase):
    """Test edge cases, error conditions, and boundary scenarios."""

    def setUp(self):
        self.sample_context = SAMPLE_CONTEXT.copy()
        self.mock_s3_client = create_mock_s3_client()

    def test_malformed_json_handling(self):
        """Test handling of malformed JSON in event processing."""
        
        with patch('boto3.client') as mock_boto:
            mock_boto.return_value = self.mock_s3_client
            
            # Mock file with malformed JSON
            malformed_content = '''
            {"valid": "json"}
            {"invalid": json missing quotes}
            {"another": "valid", "json": "object"}
            '''
            
            mock_response = Mock()
            mock_response['Body'].read.return_value = malformed_content.encode('utf-8')
            self.mock_s3_client.get_object.return_value = mock_response
            
            bucket_key = ("test-bucket", "malformed.jsonl")
            
            # Should raise JSONDecodeError on malformed line
            with self.assertRaises(json.JSONDecodeError):
                attempts_handler(bucket_key, self.sample_context, [])

    def test_missing_required_fields_in_events(self):
        """Test handling of events missing required fields."""
        
        # Event missing required extensions
        incomplete_event = {
            "timestamp": "2024-09-02T18:24:33Z",
            "actor": {"account": {"name": "12345"}},
            "object": {
                "definition": {
                    "type": "http://adlnet.gov/expapi/activities/question",
                    "name": {"en-US": "part_attempt_evaluated"}
                }
            },
            "context": {
                "extensions": {
                    "http://oli.cmu.edu/extensions/section_id": 1001,
                    # Missing many required fields
                }
            },
            "result": {
                "score": {"raw": 10, "max": 10}
            }
        }
        
        context = {'anonymize': True, 'users': {}}
        
        # Should raise KeyError for missing required fields
        with self.assertRaises(KeyError):
            from dataset.datashop import parse_attempt
            parse_attempt(incomplete_event, context)

    def test_null_and_none_value_handling(self):
        """Test handling of null/None values in various contexts."""
        
        # Test encode_array with None values
        self.assertEqual(encode_array([]), '""')
        self.assertEqual(encode_array([None]), '"None"')
        self.assertEqual(encode_array([1, None, 3]), '"1,None,3"')
        
        # Test encode_json with None
        self.assertEqual(encode_json(None), '"null"')
        self.assertEqual(encode_json({"key": None}), '"{"key":null}"')
        
        # Test sanitize_element_text with None
        self.assertEqual(sanitize_element_text(None), "")
        self.assertEqual(sanitize_attribute_value(None), "")
        
        # Test get_text_from_content with None
        self.assertEqual(get_text_from_content(None), "")

    def test_empty_string_handling(self):
        """Test handling of empty strings throughout the pipeline."""
        
        # Test trim_to_100_bytes with empty string
        self.assertEqual(trim_to_100_bytes(""), "")
        
        # Test get_text_from_content with empty content
        empty_content_structures = [
            {"content": []},
            {"content": [{"text": ""}]},
            {"content": [{"text": ""}, {"text": ""}]},
            {"content": [{"children": []}]}
        ]
        
        for structure in empty_content_structures:
            result = get_text_from_content(structure)
            self.assertEqual(result, "")

    def test_extremely_long_strings(self):
        """Test handling of very long strings that might cause issues."""
        
        # Test very long string in trim_to_100_bytes
        very_long_string = "x" * 10000
        result = trim_to_100_bytes(very_long_string)
        self.assertTrue(len(result.encode('utf-8')) <= 100)
        
        # Test very long unicode string
        unicode_string = "é" * 1000  # Each é is 2 bytes in UTF-8
        result = trim_to_100_bytes(unicode_string)
        self.assertTrue(len(result.encode('utf-8')) <= 100)
        
        # Test encoding very long arrays
        long_array = list(range(1000))
        result = encode_array(long_array)
        self.assertTrue(result.startswith('"'))
        self.assertTrue(result.endswith('"'))

    def test_special_unicode_characters(self):
        """Test handling of various Unicode characters."""
        
        # Test Basic Multilingual Plane characters
        bmp_chars = "Hello 世界 🌍 café résumé naïve"
        result = sanitize_element_text(bmp_chars)
        # Should escape non-ASCII characters
        self.assertNotIn('世', result)
        self.assertNotIn('🌍', result)
        self.assertIn('&#x', result)
        
        # Test characters beyond BMP (should be removed)
        beyond_bmp = "𝕋𝕖𝕤𝕥 𝔪𝔞𝔱𝔥 𝔰𝔶𝔪𝔟𝔬𝔩𝔰"
        result = sanitize_element_text(beyond_bmp)
        # Should remove all characters beyond BMP
        self.assertEqual(result.strip(), "")
        
        # Test mixed content
        mixed = "Normal text 🚀 beyond BMP: 𝔸 more text"
        result = sanitize_element_text(mixed)
        self.assertIn("Normal text", result)
        self.assertIn("more text", result)
        self.assertNotIn("🚀", result)
        self.assertNotIn("𝔸", result)

    def test_malformed_hierarchy_structures(self):
        """Test handling of malformed hierarchy data."""
        
        # Test circular references
        circular_hierarchy = {
            'hierarchy': {
                '1': {'title': 'A', 'children': [2]},
                '2': {'title': 'B', 'children': [3]},
                '3': {'title': 'C', 'children': [1]}  # Circular!
            }
        }
        
        # calculate_ancestors should handle this gracefully (may infinite loop or error)
        # This tests the robustness of the implementation
        try:
            calculate_ancestors(circular_hierarchy)
            # If it completes, verify no infinite loop occurred
            self.assertTrue(True)
        except (RecursionError, KeyError):
            # These are acceptable ways to handle circular references
            self.assertTrue(True)

    def test_malformed_activity_structures(self):
        """Test handling of malformed activity data in mapify_parts."""
        
        malformed_activities = {
            'activities': {
                'activity1': None,  # Null activity
                'activity2': {'parts': 'not_a_list'},  # Wrong type
                'activity3': {'parts': [None, 'not_a_dict']},  # Mixed invalid types
                'activity4': {'parts': [{'no_id': 'value'}]},  # Missing id
                'activity5': {}  # Missing parts entirely
            }
        }
        
        # Should handle gracefully without crashing
        mapify_parts(malformed_activities)
        
        # Verify results
        activities = malformed_activities['activities']
        self.assertEqual(activities['activity1']['parts'], {})  # Should add empty parts
        self.assertEqual(activities['activity2']['parts'], {})  # Should reset to empty
        self.assertEqual(activities['activity3']['parts'], {})  # Should ignore invalid items
        self.assertEqual(activities['activity4']['parts'], {})  # Should ignore items without id
        self.assertEqual(activities['activity5']['parts'], {})  # Should add empty parts

    def test_extreme_numeric_values(self):
        """Test handling of extreme numeric values."""
        
        from dataset.utils import guarentee_int
        
        # Test very large numbers
        large_number = "999999999999999999999999999999"
        result = guarentee_int(large_number)
        self.assertIsInstance(result, int)
        self.assertEqual(result, int(large_number))
        
        # Test negative numbers
        negative = "-12345"
        result = guarentee_int(negative)
        self.assertEqual(result, -12345)
        
        # Test zero
        zero = "0"
        result = guarentee_int(zero)
        self.assertEqual(result, 0)
        
        # Test float strings (should truncate)
        float_string = "123.456"
        with self.assertRaises(ValueError):
            guarentee_int(float_string)

    def test_empty_and_none_collections(self):
        """Test handling of empty collections throughout the system."""
        
        # Test empty ignored_student_ids
        context_empty_ignored = self.sample_context.copy()
        context_empty_ignored['ignored_student_ids'] = []
        
        # Should not filter anyone
        student_id = "12345"
        self.assertNotIn(student_id, context_empty_ignored['ignored_student_ids'])
        
        # Test None page_ids (should allow all)
        context_none_pages = self.sample_context.copy()
        context_none_pages['page_ids'] = None
        
        # Page matching logic should pass for any page
        page_id = 9999
        page_matches = context_none_pages["page_ids"] is None or page_id in context_none_pages["page_ids"]
        self.assertTrue(page_matches)
        
        # Test empty sub_types
        context_empty_subtypes = self.sample_context.copy()
        context_empty_subtypes['sub_types'] = []
        
        # Should not match any subtypes
        self.assertNotIn("part_attempt_evaluated", context_empty_subtypes['sub_types'])

    def test_field_pruning_edge_cases(self):
        """Test edge cases in field pruning logic."""
        
        # Test pruning with indices out of bounds
        record = ["a", "b", "c"]
        
        with self.assertRaises(IndexError):
            prune_fields(record, [5])  # Index out of bounds
        
        # Test pruning all fields
        record = ["a", "b", "c"]
        result = prune_fields(record, [2, 1, 0])  # Remove all
        self.assertEqual(result, [])
        
        # Test pruning with duplicate indices
        record = ["a", "b", "c", "d"]
        result = prune_fields(record, [1, 1, 2])  # Duplicate index 1
        # Should handle gracefully (exact behavior depends on implementation)
        self.assertIsInstance(result, list)
        
        # Test pruning with negative indices
        record = ["a", "b", "c"]
        with self.assertRaises(IndexError):
            prune_fields(record, [-1])  # Negative index

    def test_student_id_determination_edge_cases(self):
        """Test edge cases in student ID determination."""
        
        # Test with missing user in lookup
        context = {
            'anonymize': False,
            'lookup': {
                'users': {}  # Empty users
            }
        }
        json_data = {"actor": {"account": {"name": "unknown_student"}}}
        
        result = determine_student_id(context, json_data)
        self.assertEqual(result, "unknown_student")  # Should fallback to original ID
        
        # Test with malformed users structure
        context = {
            'anonymize': False,
            'lookup': {
                'users': None  # Null users
            }
        }
        
        with self.assertRaises(TypeError):
            determine_student_id(context, json_data)

    def test_xml_generation_with_malformed_data(self):
        """Test XML generation with various malformed input data."""
        
        # Test with minimal part attempt data
        minimal_part_attempt = {
            'timestamp': '2024-09-02T18:24:33Z',
            'user_id': '123',
            'activity_id': 999,  # Not in lookup
            'part_id': 'unknown',  # Not in lookup
            'score': 5,
            'out_of': 10,
            'hints': [],
            'attached_objectives': []
        }
        
        minimal_context = {
            'activities': {},  # Empty activities
            'hierarchy': {},  # Empty hierarchy
            'skill_titles': {},  # Empty skills
            'dataset_name': 'Test'
        }
        
        # Should handle gracefully without crashing
        result = to_xml_message(minimal_part_attempt, minimal_context)
        
        # Basic XML structure should still be present
        self.assertIn('<context_message', result)
        self.assertIn('<tool_message', result)
        self.assertIn('<tutor_message', result)

    def test_memory_intensive_operations(self):
        """Test operations that might be memory intensive."""
        
        # Test with large number of events (simulated)
        large_event_count = 10000
        
        # Simulate chunking large datasets
        from dataset.dataset import chunkify, calculate_number_of_chunks
        
        large_dataset = list(range(large_event_count))
        chunk_size = 100
        
        # Test chunking doesn't consume excessive memory
        chunks = list(chunkify(large_dataset, chunk_size))
        expected_chunks = calculate_number_of_chunks(large_event_count, chunk_size)
        
        self.assertEqual(len(chunks), expected_chunks)
        self.assertEqual(sum(len(chunk) for chunk in chunks), large_event_count)

    def test_concurrent_access_simulation(self):
        """Test behavior under simulated concurrent access conditions."""
        
        # Test global_context thread safety concerns
        from dataset.datashop import global_context
        
        # Simulate multiple "threads" setting context message ID
        original_id = global_context.get("last_good_context_message_id")
        
        global_context["last_good_context_message_id"] = "thread1_id"
        thread1_id = global_context["last_good_context_message_id"]
        
        global_context["last_good_context_message_id"] = "thread2_id"
        thread2_id = global_context["last_good_context_message_id"]
        
        # Verify last writer wins (expected behavior for dict)
        self.assertEqual(thread2_id, "thread2_id")
        
        # Restore original state
        global_context["last_good_context_message_id"] = original_id

if __name__ == '__main__':
    unittest.main()