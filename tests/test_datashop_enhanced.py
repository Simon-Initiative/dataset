import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import xml.etree.ElementTree as ET
from dataset.datashop import (
    process_jsonl_file, process_part_attempts, parse_attempt, to_xml_message,
    expand_context, create_hint_message_pairs, sanitize_element_text, sanitize_attribute_value,
    context_message, tool_message, tutor_message, get_hints_for_part, get_text_from_content,
    trim_to_100_bytes, assemble_from_hierarchy_path, global_context
)
from tests.test_data import (
    SAMPLE_PART_ATTEMPT_EVENT, SAMPLE_LOOKUP_DATA, SAMPLE_CONTEXT, 
    create_mock_s3_client, create_sample_part_attempt
)

class TestDatashopEnhanced(unittest.TestCase):

    def setUp(self):
        self.sample_context = SAMPLE_CONTEXT.copy()
        self.sample_lookup = SAMPLE_LOOKUP_DATA.copy()
        self.mock_s3_client = create_mock_s3_client()
        # Reset global context for each test
        global_context["last_good_context_message_id"] = None

    @patch('boto3.client')
    def test_process_jsonl_file_success(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        # Setup context with lookup data
        context = self.sample_context.copy()
        context['lookup'] = self.sample_lookup
        
        # Create JSONL content
        jsonl_content = json.dumps(SAMPLE_PART_ATTEMPT_EVENT)
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        result = process_jsonl_file(bucket_key, context, [])
        
        # Verify result structure
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        
        # Verify part attempt structure
        part_attempt = result[0]
        self.assertIn('timestamp', part_attempt)
        self.assertIn('user_id', part_attempt)
        self.assertIn('activity_id', part_attempt)
        self.assertIn('session_id', part_attempt)

    @patch('boto3.client')
    def test_process_jsonl_file_filtering(self, mock_boto_client):
        mock_boto_client.return_value = self.mock_s3_client
        
        context = self.sample_context.copy()
        context['lookup'] = self.sample_lookup
        
        # Create event with ignored student
        ignored_event = SAMPLE_PART_ATTEMPT_EVENT.copy()
        ignored_event["actor"]["account"]["name"] = 99999  # In ignored list
        
        jsonl_content = json.dumps(ignored_event)
        mock_response = Mock()
        mock_response['Body'].read.return_value = jsonl_content.encode('utf-8')
        self.mock_s3_client.get_object.return_value = mock_response
        
        bucket_key = ("test-bucket", "test-key.jsonl")
        result = process_jsonl_file(bucket_key, context, [])
        
        # Should be filtered out
        self.assertEqual(len(result), 0)

    def test_process_part_attempts_success(self):
        context = self.sample_context.copy()
        context['lookup'] = self.sample_lookup
        
        part_attempts = [create_sample_part_attempt()]
        
        result = process_part_attempts(part_attempts, context)
        
        # Verify XML output
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        self.assertIn('<tutor_message', result[0])
        self.assertIn('<context_message', result[0])

    def test_parse_attempt_data_extraction(self):
        context = {'anonymize': False, 'users': {}}
        
        result = parse_attempt(SAMPLE_PART_ATTEMPT_EVENT, context)
        
        # Verify all expected fields
        expected_fields = [
            'timestamp', 'user_id', 'section_id', 'project_id', 'publication_id',
            'page_id', 'activity_id', 'activity_revision_id', 'attached_objectives',
            'page_attempt_guid', 'page_attempt_number', 'part_id', 'part_attempt_guid',
            'part_attempt_number', 'activity_attempt_number', 'activity_attempt_guid',
            'score', 'out_of', 'hints', 'response', 'feedback'
        ]
        
        for field in expected_fields:
            self.assertIn(field, result, f"Missing field: {field}")

    def test_parse_attempt_score_extraction(self):
        context = {'anonymize': True, 'users': {}}
        
        result = parse_attempt(SAMPLE_PART_ATTEMPT_EVENT, context)
        
        self.assertEqual(result['score'], 10)
        self.assertEqual(result['out_of'], 10)

    def test_expand_context_integration(self):
        part_attempt = create_sample_part_attempt()
        context = self.sample_lookup.copy()
        
        expanded = expand_context(context, part_attempt)
        
        # Verify expanded fields
        self.assertIn('time', expanded)
        self.assertIn('user_id', expanded)
        self.assertIn('session_id', expanded)
        self.assertIn('context_message_id', expanded)
        self.assertIn('problem_name', expanded)
        self.assertIn('total_hints_available', expanded)
        
        # Verify problem name format
        expected_problem_name = f"Activity {part_attempt['activity_id']}, Part {part_attempt['part_id']}"
        self.assertEqual(expanded['problem_name'], expected_problem_name)

    def test_expand_context_hint_counting(self):
        part_attempt = create_sample_part_attempt()
        
        # Setup context with hints
        context = {
            'activities': {
                '5001': {
                    'parts': {
                        'part1': {
                            'hints': [
                                {'id': 'h1', 'content': [{'text': 'Hint 1'}]},
                                {'id': 'h2', 'content': [{'text': 'Hint 2'}]},
                                {'id': 'h3', 'content': [{'text': ''}]}  # Empty hint
                            ]
                        }
                    }
                }
            }
        }
        
        expanded = expand_context(context, part_attempt)
        
        # Should count only non-empty hints
        self.assertEqual(expanded['total_hints_available'], 2)

    def test_create_hint_message_pairs_with_hints(self):
        part_attempt = create_sample_part_attempt()
        part_attempt['hints'] = ['hint1', 'hint2']
        
        context = {
            'activities': {
                '5001': {
                    'parts': {
                        'part1': {
                            'hints': [
                                {'id': 'hint1', 'content': [{'text': 'First hint'}]},
                                {'id': 'hint2', 'content': [{'text': 'Second hint'}]}
                            ]
                        }
                    }
                }
            }
        }
        
        result = create_hint_message_pairs(part_attempt, context)
        
        # Should create pairs of tool/tutor messages for each hint
        self.assertEqual(len(result), 4)  # 2 hints × 2 messages each
        self.assertIn('<tool_message', result[0])
        self.assertIn('<tutor_message', result[1])

    def test_create_hint_message_pairs_no_hints(self):
        part_attempt = create_sample_part_attempt()
        part_attempt['hints'] = []
        
        context = {'activities': {'5001': {'parts': {'part1': {'hints': []}}}}}
        
        result = create_hint_message_pairs(part_attempt, context)
        
        # Should return empty list
        self.assertEqual(len(result), 0)

    def test_sanitize_element_text_basic(self):
        result = sanitize_element_text("Hello, World!")
        self.assertEqual(result, "Hello, World!")

    def test_sanitize_element_text_non_ascii(self):
        result = sanitize_element_text("Café résumé")
        # Should convert non-ASCII to hex entities
        self.assertIn('&#x', result)
        self.assertNotIn('é', result)

    def test_sanitize_element_text_high_unicode(self):
        # Test character beyond BMP (Basic Multilingual Plane)
        high_unicode = "𝕋𝕖𝕤𝕥"  # Mathematical symbols
        result = sanitize_element_text(high_unicode)
        # Should remove characters beyond BMP
        self.assertEqual(result, "")

    def test_sanitize_element_text_empty_and_none(self):
        self.assertEqual(sanitize_element_text(""), "")
        self.assertEqual(sanitize_element_text(None), "")

    def test_sanitize_attribute_value_consistency(self):
        test_string = "Test with special chars: <>&\""
        element_result = sanitize_element_text(test_string)
        attribute_result = sanitize_attribute_value(test_string)
        
        # Both should handle the same way for non-Unicode characters
        # (Note: XML escaping of <>&" would be handled by ET.tostring)
        self.assertEqual(element_result, attribute_result)

    def test_context_message_creation(self):
        context = {
            'context_message_id': 'test-msg-123',
            'user_id': '12345',
            'session_id': 'session-456',
            'time': '2024-09-02T18:24:33Z',
            'dataset_name': 'Test Dataset',
            'hierarchy': {'4001': {'title': 'Test Page', 'ancestors': [], 'graded': True}},
            'problem_name': 'Test Problem'
        }
        
        result = context_message("START_PROBLEM", context)
        
        # Verify XML structure
        self.assertIn('<context_message', result)
        self.assertIn('context_message_id="test-msg-123"', result)
        self.assertIn('name="START_PROBLEM"', result)
        self.assertIn('<meta>', result)
        self.assertIn('<dataset>', result)

    def test_tool_message_creation(self):
        context = {
            'context_message_id': 'test-msg-123',
            'user_id': '12345',
            'session_id': 'session-456',
            'time': '2024-09-02T18:24:33Z',
            'transaction_id': 'trans-789',
            'problem_name': 'Test Problem'
        }
        
        result = tool_message("ATTEMPT", "ATTEMPT", context)
        
        # Verify XML structure
        self.assertIn('<tool_message', result)
        self.assertIn('<meta>', result)
        self.assertIn('<semantic_event', result)
        self.assertIn('<event_descriptor>', result)

    def test_tutor_message_creation(self):
        context = {
            'context_message_id': 'test-msg-123',
            'user_id': '12345',
            'session_id': 'session-456',
            'time': '2024-09-02T18:24:33Z',
            'transaction_id': 'trans-789',
            'problem_name': 'Test Problem',
            'part_attempt': create_sample_part_attempt(),
            'skill_ids': [101, 102],
            'skill_titles': {'101': 'Math', '102': 'Problem Solving'}
        }
        
        result = tutor_message("RESULT", context)
        
        # Verify XML structure
        self.assertIn('<tutor_message', result)
        self.assertIn('<meta>', result)
        self.assertIn('<semantic_event', result)
        self.assertIn('<action_evaluation>', result)
        self.assertIn('<skill>', result)

    def test_tutor_message_with_hint(self):
        context = {
            'context_message_id': 'test-msg-123',
            'user_id': '12345',
            'session_id': 'session-456',
            'time': '2024-09-02T18:24:33Z',
            'transaction_id': 'trans-789',
            'problem_name': 'Test Problem',
            'current_hint_number': 1,
            'total_hints_available': 2,
            'hint_text': 'This is a helpful hint',
            'skill_ids': [],
            'skill_titles': {}
        }
        
        result = tutor_message("HINT_MSG", context)
        
        # Verify hint-specific elements
        self.assertIn('<tutor_advice>', result)
        self.assertIn('This is a helpful hint', result)

    def test_get_hints_for_part_success(self):
        part_attempt = create_sample_part_attempt()
        part_attempt['hints'] = ['hint1', 'hint2']
        
        context = {
            'activities': {
                '5001': {
                    'parts': {
                        'part1': {
                            'hints': [
                                {'id': 'hint1', 'content': [{'text': 'First hint text'}]},
                                {'id': 'hint2', 'content': [{'text': 'Second hint text'}]}
                            ]
                        }
                    }
                }
            }
        }
        
        result = get_hints_for_part(part_attempt, context)
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], 'First hint text')
        self.assertEqual(result[1], 'Second hint text')

    def test_get_hints_for_part_missing_hint(self):
        part_attempt = create_sample_part_attempt()
        part_attempt['hints'] = ['hint1', 'nonexistent_hint']
        
        context = {
            'activities': {
                '5001': {
                    'parts': {
                        'part1': {
                            'hints': [
                                {'id': 'hint1', 'content': [{'text': 'First hint text'}]}
                            ]
                        }
                    }
                }
            }
        }
        
        result = get_hints_for_part(part_attempt, context)
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], 'First hint text')
        self.assertEqual(result[1], 'Unknown hint')  # Default for missing hint

    def test_get_hints_for_part_malformed_activity_structure(self):
        part_attempt = create_sample_part_attempt()
        part_attempt['hints'] = ['hint1']
        
        # Test various malformed structures
        malformed_contexts = [
            {'activities': {}},  # Missing activity
            {'activities': {'5001': {}}},  # Missing parts
            {'activities': {'5001': {'parts': None}}},  # None parts
            {'activities': {'5001': {'parts': {'part1': {}}}}},  # Missing hints
            {'activities': {'5001': {'parts': {'part1': {'hints': None}}}}},  # None hints
        ]
        
        for context in malformed_contexts:
            result = get_hints_for_part(part_attempt, context)
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0], 'Unknown hint')

    def test_get_text_from_content_simple(self):
        content = {'content': [{'text': 'Simple text'}]}
        result = get_text_from_content(content)
        self.assertEqual(result, 'Simple text')

    def test_get_text_from_content_nested(self):
        content = {
            'content': [
                {'text': 'First part '},
                {
                    'text': 'second part ',
                    'children': [
                        {'text': 'nested text'}
                    ]
                }
            ]
        }
        result = get_text_from_content(content)
        self.assertEqual(result, 'First part second part nested text')

    def test_get_text_from_content_none_and_empty(self):
        self.assertEqual(get_text_from_content(None), '')
        self.assertEqual(get_text_from_content({'content': []}), '')
        self.assertEqual(get_text_from_content({}), '')

    def test_trim_to_100_bytes_ascii(self):
        short_text = "Short text"
        result = trim_to_100_bytes(short_text)
        self.assertEqual(result, short_text)
        
        long_text = "x" * 150
        result = trim_to_100_bytes(long_text)
        self.assertEqual(len(result.encode('utf-8')), 100)

    def test_trim_to_100_bytes_unicode(self):
        # Unicode characters take multiple bytes
        unicode_text = "é" * 60  # Each é is 2 bytes
        result = trim_to_100_bytes(unicode_text)
        self.assertTrue(len(result.encode('utf-8')) <= 100)
        self.assertTrue(len(result) <= 60)  # Should be less than original

    def test_assemble_from_hierarchy_path_simple(self):
        hierarchy = {
            '4001': {
                'title': 'Test Page',
                'graded': True,
                'ancestors': []
            }
        }
        
        result = assemble_from_hierarchy_path(4001, "Test Problem", hierarchy)
        
        # Should be an XML element
        self.assertIsInstance(result, ET.Element)
        self.assertEqual(result.tag, 'level')
        self.assertEqual(result.get('type'), 'Page')

    def test_assemble_from_hierarchy_path_with_ancestors(self):
        hierarchy = {
            '4001': {
                'title': 'Test Page',
                'graded': True,
                'ancestors': [24, 25]
            },
            '24': {
                'title': 'Test Unit',
                'children': [25]
            },
            '25': {
                'title': 'Test Module',
                'children': [4001]
            }
        }
        
        result = assemble_from_hierarchy_path(4001, "Test Problem", hierarchy)
        
        # Should create nested structure
        self.assertEqual(result.tag, 'level')
        self.assertEqual(result.get('type'), 'Container')
        
        # Find the nested page level
        nested_levels = result.findall('.//level[@type="Page"]')
        self.assertEqual(len(nested_levels), 1)

    def test_to_xml_message_integration(self):
        part_attempt = create_sample_part_attempt()
        
        context = self.sample_lookup.copy()
        context['dataset_name'] = 'Test Dataset'
        
        result = to_xml_message(part_attempt, context)
        
        # Verify complete XML structure
        self.assertIn('<context_message', result)
        self.assertIn('<tool_message', result)
        self.assertIn('<tutor_message', result)
        
        # Should contain START_PROBLEM context message
        self.assertIn('name="START_PROBLEM"', result)
        
        # Should not contain numeric entities that need unescaping
        self.assertNotIn('&amp;#x', result)

    def test_global_context_message_id_tracking(self):
        part_attempt1 = create_sample_part_attempt()
        part_attempt1['part_attempt_number'] = 1
        part_attempt1['activity_attempt_number'] = 1
        
        part_attempt2 = create_sample_part_attempt()
        part_attempt2['part_attempt_number'] = 2
        part_attempt2['activity_attempt_number'] = 1
        
        context = self.sample_lookup.copy()
        context['dataset_name'] = 'Test Dataset'
        
        # Reset global context
        global_context["last_good_context_message_id"] = None
        
        # First attempt should create START_PROBLEM
        result1 = to_xml_message(part_attempt1, context)
        self.assertIn('name="START_PROBLEM"', result1)
        
        # Second attempt should not create START_PROBLEM (context ID is set)
        result2 = to_xml_message(part_attempt2, context)
        self.assertNotIn('name="START_PROBLEM"', result2)

if __name__ == '__main__':
    unittest.main()