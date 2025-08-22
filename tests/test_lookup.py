import unittest
from unittest.mock import Mock, patch, MagicMock
import json
from dataset.lookup import retrieve_lookup, determine_student_id, calculate_ancestors, mapify_parts, post_process
from tests.test_data import create_mock_s3_client, SAMPLE_LOOKUP_DATA, SAMPLE_CONTEXT

class TestLookup(unittest.TestCase):

    def setUp(self):
        self.sample_lookup = SAMPLE_LOOKUP_DATA.copy()
        self.sample_context = SAMPLE_CONTEXT.copy()

    @patch('boto3.client')
    def test_retrieve_lookup_success(self, mock_boto_client):
        mock_s3_client = create_mock_s3_client()
        mock_boto_client.return_value = mock_s3_client
        
        result = retrieve_lookup(mock_s3_client, self.sample_context)
        
        # Verify S3 call was made with correct parameters
        mock_s3_client.get_object.assert_called_once_with(
            Bucket=self.sample_context["results_bucket_name"],
            Key=f"contexts/{self.sample_context['job_id']}.json"
        )
        
        # Verify post-processing was applied
        self.assertIn('activities', result)
        self.assertIsInstance(result['activities'], dict)

    def test_determine_student_id_anonymized(self):
        context = {'anonymize': True, 'lookup': {}}
        json_data = {"actor": {"account": {"name": "student123"}}}
        
        result = determine_student_id(context, json_data)
        
        self.assertEqual(result, "student123")

    def test_determine_student_id_not_anonymized_with_email(self):
        context = {
            'anonymize': False, 
            'lookup': {
                'users': {
                    'student123': {'email': 'student@example.com'}
                }
            }
        }
        json_data = {"actor": {"account": {"name": "student123"}}}
        
        result = determine_student_id(context, json_data)
        
        self.assertEqual(result, 'student@example.com')

    def test_determine_student_id_not_anonymized_fallback_to_id(self):
        context = {
            'anonymize': False, 
            'lookup': {
                'users': {
                    'other_student': {'email': 'other@example.com'}
                }
            }
        }
        json_data = {"actor": {"account": {"name": "student123"}}}
        
        result = determine_student_id(context, json_data)
        
        # Should fallback to user ID when not found in lookup
        self.assertEqual(result, "student123")

    def test_determine_student_id_not_anonymized_no_email(self):
        context = {
            'anonymize': False, 
            'lookup': {
                'users': {
                    'student123': {'name': 'Student Name'}  # No email field
                }
            }
        }
        json_data = {"actor": {"account": {"name": "student123"}}}
        
        result = determine_student_id(context, json_data)
        
        # Should fallback to user ID when email not available
        self.assertEqual(result, "student123")

    def test_calculate_ancestors_simple_hierarchy(self):
        context = {
            'hierarchy': {
                '1': {'title': 'Root', 'children': [2]},
                '2': {'title': 'Child', 'children': [3]},
                '3': {'title': 'Grandchild', 'children': []}
            }
        }
        
        calculate_ancestors(context)
        
        # Check that parents are calculated correctly
        self.assertEqual(context['hierarchy']['2']['parent'], '1')
        self.assertEqual(context['hierarchy']['3']['parent'], '2')
        
        # Check ancestors are calculated correctly
        self.assertEqual(context['hierarchy']['1']['ancestors'], [])
        self.assertEqual(context['hierarchy']['2']['ancestors'], [1])
        self.assertEqual(context['hierarchy']['3']['ancestors'], [1, 2])

    def test_calculate_ancestors_complex_hierarchy(self):
        context = {
            'hierarchy': {
                '10': {'title': 'Course', 'children': [20, 30]},
                '20': {'title': 'Unit 1', 'children': [40]},
                '30': {'title': 'Unit 2', 'children': [50]},
                '40': {'title': 'Page 1', 'children': []},
                '50': {'title': 'Page 2', 'children': []}
            }
        }
        
        calculate_ancestors(context)
        
        # Verify branching hierarchy
        self.assertEqual(context['hierarchy']['40']['ancestors'], [10, 20])
        self.assertEqual(context['hierarchy']['50']['ancestors'], [10, 30])

    def test_calculate_ancestors_no_children(self):
        context = {
            'hierarchy': {
                '1': {'title': 'Only Node'}
            }
        }
        
        calculate_ancestors(context)
        
        # Should not crash and should have empty ancestors
        self.assertEqual(context['hierarchy']['1']['ancestors'], [])

    def test_mapify_parts_with_list_of_parts(self):
        context = {
            'activities': {
                'activity1': {
                    'parts': [
                        {'id': 'part1', 'content': 'Part 1 content'},
                        {'id': 'part2', 'content': 'Part 2 content'}
                    ]
                }
            }
        }
        
        mapify_parts(context)
        
        # Verify parts are converted to dictionary
        activity = context['activities']['activity1']
        self.assertIsInstance(activity['parts'], dict)
        self.assertIn('part1', activity['parts'])
        self.assertIn('part2', activity['parts'])
        self.assertEqual(activity['parts']['part1']['content'], 'Part 1 content')

    def test_mapify_parts_with_dict_already(self):
        context = {
            'activities': {
                'activity1': {
                    'parts': {'part1': {'content': 'Already a dict'}}
                }
            }
        }
        
        original_parts = context['activities']['activity1']['parts'].copy()
        mapify_parts(context)
        
        # Should reset to empty dict then process (but no items to add)
        self.assertIsInstance(context['activities']['activity1']['parts'], dict)

    def test_mapify_parts_with_none_parts(self):
        context = {
            'activities': {
                'activity1': {
                    'parts': None
                }
            }
        }
        
        mapify_parts(context)
        
        # Should handle None gracefully
        self.assertEqual(context['activities']['activity1']['parts'], {})

    def test_mapify_parts_with_invalid_part_structure(self):
        context = {
            'activities': {
                'activity1': {
                    'parts': [
                        {'id': 'part1', 'content': 'Valid part'},
                        {'content': 'Part without id'},  # Missing id
                        'invalid_part',  # Not a dict
                        {'id': None, 'content': 'None id'}  # None id
                    ]
                }
            }
        }
        
        mapify_parts(context)
        
        # Should only include valid parts
        activity = context['activities']['activity1']
        self.assertIn('part1', activity['parts'])
        self.assertEqual(len(activity['parts']), 1)

    def test_mapify_parts_missing_parts_key(self):
        context = {
            'activities': {
                'activity1': {
                    'title': 'Activity without parts'
                }
            }
        }
        
        mapify_parts(context)
        
        # Should add empty parts dict
        self.assertEqual(context['activities']['activity1']['parts'], {})

    def test_post_process_integration(self):
        context = {
            'activities': {
                'activity1': {
                    'parts': [
                        {'id': 'part1', 'hints': ['hint1']}
                    ]
                }
            },
            'hierarchy': {
                '1': {'title': 'Root', 'children': [2]},
                '2': {'title': 'Child', 'children': []}
            }
        }
        
        result = post_process(context)
        
        # Verify both transformations were applied
        self.assertIsInstance(result['activities']['activity1']['parts'], dict)
        self.assertIn('part1', result['activities']['activity1']['parts'])
        self.assertEqual(result['hierarchy']['2']['parent'], '1')
        self.assertEqual(result['hierarchy']['2']['ancestors'], [1])

    def test_post_process_returns_modified_context(self):
        original_context = {'activities': {}, 'hierarchy': {}}
        result = post_process(original_context)
        
        # Verify same object is returned (modified in place)
        self.assertIs(result, original_context)

if __name__ == '__main__':
    unittest.main()