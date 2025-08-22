import unittest
from unittest.mock import Mock, patch, MagicMock
import argparse
import sys
from io import StringIO

class TestJob(unittest.TestCase):

    def setUp(self):
        # Store original argv
        self.original_argv = sys.argv.copy()
        
    def tearDown(self):
        # Restore original argv
        sys.argv = self.original_argv

    def create_test_args(self, overrides=None):
        """Create standard test arguments with optional overrides."""
        args = [
            'job.py',
            '--bucket_name', 'test-bucket',
            '--action', 'attempt_evaluated',
            '--job_id', 'test-job-123',
            '--section_ids', '1001,1002',
            '--page_ids', '4001,4002',
            '--chunk_size', '100'
        ]
        
        if overrides:
            args.extend(overrides)
            
        return args

    @patch('dataset.dataset.generate_dataset')
    def test_job_regular_dataset_generation(self, mock_generate_dataset):
        # Setup command line arguments
        test_args = self.create_test_args()
        
        with patch.object(sys, 'argv', test_args):
            # Import and run job module
            import job
            
            # Verify generate_dataset was called with correct parameters
            mock_generate_dataset.assert_called_once()
            call_args = mock_generate_dataset.call_args[0]
            call_kwargs = mock_generate_dataset.call_args[1]
            
            # Check section_ids parameter
            self.assertEqual(call_args[0], [1001, 1002])
            
            # Check action parameter
            self.assertEqual(call_args[1], "attempt_evaluated")
            
            # Check context parameter
            context = call_args[2]
            self.assertEqual(context['bucket_name'], 'test-bucket')
            self.assertEqual(context['job_id'], 'test-job-123')
            self.assertEqual(context['section_ids'], [1001, 1002])
            self.assertEqual(context['page_ids'], [4001, 4002])
            self.assertEqual(context['chunk_size'], 100)

    @patch('dataset.dataset.generate_datashop')
    def test_job_datashop_generation(self, mock_generate_datashop):
        # Setup command line arguments for datashop
        test_args = self.create_test_args(['--action', 'datashop'])
        
        with patch.object(sys, 'argv', test_args):
            # Import and run job module
            import job
            
            # Verify generate_datashop was called
            mock_generate_datashop.assert_called_once()
            call_args = mock_generate_datashop.call_args[0]
            
            # Check context parameter
            context = call_args[0]
            self.assertEqual(context['action'], 'datashop')

    def test_argument_parsing_section_ids(self):
        test_args = self.create_test_args(['--section_ids', '1,2,3,4,5'])
        
        with patch.object(sys, 'argv', test_args), \
             patch('dataset.dataset.generate_dataset') as mock_generate:
            
            import job
            
            call_args = mock_generate.call_args[0]
            section_ids = call_args[0]
            self.assertEqual(section_ids, [1, 2, 3, 4, 5])

    def test_argument_parsing_ignored_student_ids(self):
        test_args = self.create_test_args(['--ignored_student_ids', '999,888,777'])
        
        with patch.object(sys, 'argv', test_args), \
             patch('dataset.dataset.generate_dataset') as mock_generate:
            
            import job
            
            context = mock_generate.call_args[0][2]
            self.assertEqual(context['ignored_student_ids'], [999, 888, 777])

    def test_argument_parsing_ignored_student_ids_empty(self):
        test_args = self.create_test_args()  # No ignored_student_ids
        
        with patch.object(sys, 'argv', test_args), \
             patch('dataset.dataset.generate_dataset') as mock_generate:
            
            import job
            
            context = mock_generate.call_args[0][2]
            self.assertEqual(context['ignored_student_ids'], [])

    def test_argument_parsing_sub_types(self):
        test_args = self.create_test_args(['--sub_types', 'part_attempt_evaluated,activity_attempt_evaluated'])
        
        with patch.object(sys, 'argv', test_args), \
             patch('dataset.dataset.generate_dataset') as mock_generate:
            
            import job
            
            context = mock_generate.call_args[0][2]
            self.assertEqual(context['sub_types'], ['part_attempt_evaluated', 'activity_attempt_evaluated'])

    def test_argument_parsing_sub_types_empty(self):
        test_args = self.create_test_args()  # No sub_types
        
        with patch.object(sys, 'argv', test_args), \
             patch('dataset.dataset.generate_dataset') as mock_generate:
            
            import job
            
            context = mock_generate.call_args[0][2]
            self.assertEqual(context['sub_types'], [])

    def test_argument_parsing_anonymize_false(self):
        test_args = self.create_test_args(['--anonymize', 'false'])
        
        with patch.object(sys, 'argv', test_args), \
             patch('dataset.dataset.generate_dataset') as mock_generate:
            
            import job
            
            context = mock_generate.call_args[0][2]
            self.assertFalse(context['anonymize'])

    def test_argument_parsing_anonymize_true(self):
        test_args = self.create_test_args(['--anonymize', 'true'])
        
        with patch.object(sys, 'argv', test_args), \
             patch('dataset.dataset.generate_dataset') as mock_generate:
            
            import job
            
            context = mock_generate.call_args[0][2]
            self.assertTrue(context['anonymize'])

    def test_argument_parsing_anonymize_default(self):
        test_args = self.create_test_args()  # No anonymize flag
        
        with patch.object(sys, 'argv', test_args), \
             patch('dataset.dataset.generate_dataset') as mock_generate:
            
            import job
            
            context = mock_generate.call_args[0][2]
            self.assertTrue(context['anonymize'])  # Default is True

    def test_argument_parsing_exclude_fields(self):
        test_args = self.create_test_args(['--exclude_fields', 'timestamp,user_id,section_id'])
        
        with patch.object(sys, 'argv', test_args), \
             patch('dataset.dataset.generate_dataset') as mock_generate:
            
            import job
            
            context = mock_generate.call_args[0][2]
            self.assertEqual(context['exclude_fields'], ['timestamp', 'user_id', 'section_id'])

    def test_argument_parsing_exclude_fields_empty(self):
        test_args = self.create_test_args()  # No exclude_fields
        
        with patch.object(sys, 'argv', test_args), \
             patch('dataset.dataset.generate_dataset') as mock_generate:
            
            import job
            
            context = mock_generate.call_args[0][2]
            self.assertEqual(context['exclude_fields'], [])

    def test_argument_parsing_page_ids_all(self):
        test_args = self.create_test_args(['--page_ids', 'all'])
        
        with patch.object(sys, 'argv', test_args), \
             patch('dataset.dataset.generate_dataset') as mock_generate:
            
            import job
            
            context = mock_generate.call_args[0][2]
            self.assertIsNone(context['page_ids'])

    def test_argument_parsing_page_ids_datashop_all(self):
        test_args = self.create_test_args(['--action', 'datashop', '--page_ids', '1,2,3'])
        
        with patch.object(sys, 'argv', test_args), \
             patch('dataset.dataset.generate_datashop') as mock_generate:
            
            import job
            
            context = mock_generate.call_args[0][0]
            self.assertIsNone(context['page_ids'])  # datashop always sets to None

    def test_argument_parsing_results_bucket_default(self):
        test_args = self.create_test_args()  # No results_bucket_name
        
        with patch.object(sys, 'argv', test_args), \
             patch('dataset.dataset.generate_dataset') as mock_generate:
            
            import job
            
            context = mock_generate.call_args[0][2]
            self.assertEqual(context['results_bucket_name'], 'torus-datasets-prod')

    def test_argument_parsing_results_bucket_custom(self):
        test_args = self.create_test_args(['--results_bucket_name', 'custom-results-bucket'])
        
        with patch.object(sys, 'argv', test_args), \
             patch('dataset.dataset.generate_dataset') as mock_generate:
            
            import job
            
            context = mock_generate.call_args[0][2]
            self.assertEqual(context['results_bucket_name'], 'custom-results-bucket')

    def test_argument_parsing_enforce_project_id_int(self):
        test_args = self.create_test_args(['--enforce_project_id', '12345'])
        
        with patch.object(sys, 'argv', test_args), \
             patch('dataset.dataset.generate_dataset') as mock_generate:
            
            import job
            
            context = mock_generate.call_args[0][2]
            self.assertEqual(context['project_id'], 12345)

    def test_argument_parsing_enforce_project_id_none(self):
        test_args = self.create_test_args()  # No enforce_project_id
        
        with patch.object(sys, 'argv', test_args), \
             patch('dataset.dataset.generate_dataset') as mock_generate:
            
            import job
            
            context = mock_generate.call_args[0][2]
            self.assertIsNone(context['project_id'])

    def test_inventory_bucket_name_construction(self):
        test_args = self.create_test_args(['--bucket_name', 'my-data-bucket'])
        
        with patch.object(sys, 'argv', test_args), \
             patch('dataset.dataset.generate_dataset') as mock_generate:
            
            import job
            
            context = mock_generate.call_args[0][2]
            self.assertEqual(context['inventory_bucket_name'], 'my-data-bucket-inventory')

    def test_context_structure_completeness(self):
        test_args = self.create_test_args([
            '--ignored_student_ids', '999',
            '--sub_types', 'part_attempt_evaluated',
            '--anonymize', 'false',
            '--exclude_fields', 'timestamp',
            '--enforce_project_id', '2001'
        ])
        
        with patch.object(sys, 'argv', test_args), \
             patch('dataset.dataset.generate_dataset') as mock_generate:
            
            import job
            
            context = mock_generate.call_args[0][2]
            
            # Verify all expected context fields are present
            expected_fields = [
                'bucket_name', 'inventory_bucket_name', 'results_bucket_name',
                'job_id', 'ignored_student_ids', 'chunk_size', 'section_ids',
                'page_ids', 'action', 'sub_types', 'exclude_fields', 
                'project_id', 'anonymize'
            ]
            
            for field in expected_fields:
                self.assertIn(field, context, f"Missing context field: {field}")

    def test_missing_required_arguments(self):
        # Test with missing required argument
        incomplete_args = ['job.py', '--bucket_name', 'test-bucket']
        
        with patch.object(sys, 'argv', incomplete_args):
            with self.assertRaises(SystemExit):
                # This should raise SystemExit due to missing required arguments
                import job

    @patch('builtins.print')
    def test_job_completion_message(self, mock_print):
        test_args = self.create_test_args()
        
        with patch.object(sys, 'argv', test_args), \
             patch('dataset.dataset.generate_dataset'):
            
            import job
            
            # Verify completion message is printed
            mock_print.assert_called_with("job completed")

    def test_guarentee_int_usage(self):
        """Test that guarentee_int is properly used for project_id."""
        
        # Test with string project ID
        test_args = self.create_test_args(['--enforce_project_id', '12345'])
        
        with patch.object(sys, 'argv', test_args), \
             patch('dataset.dataset.generate_dataset') as mock_generate, \
             patch('dataset.utils.guarentee_int', side_effect=lambda x: int(x) if x else None) as mock_guarentee:
            
            import job
            
            # Verify guarentee_int was called
            mock_guarentee.assert_called_once_with('12345')
            
            context = mock_generate.call_args[0][2]
            self.assertEqual(context['project_id'], 12345)

    def test_action_routing_logic(self):
        """Test that datashop and regular actions are routed correctly."""
        
        # Test regular action
        test_args = self.create_test_args(['--action', 'page_viewed'])
        
        with patch.object(sys, 'argv', test_args), \
             patch('dataset.dataset.generate_dataset') as mock_generate_dataset, \
             patch('dataset.dataset.generate_datashop') as mock_generate_datashop:
            
            import job
            
            mock_generate_dataset.assert_called_once()
            mock_generate_datashop.assert_not_called()
            
        # Reset mocks and test datashop action
        mock_generate_dataset.reset_mock()
        mock_generate_datashop.reset_mock()
        
        test_args = self.create_test_args(['--action', 'datashop'])
        
        with patch.object(sys, 'argv', test_args), \
             patch('dataset.dataset.generate_dataset') as mock_generate_dataset, \
             patch('dataset.dataset.generate_datashop') as mock_generate_datashop:
            
            # Need to reload the module to re-execute the main block
            import importlib
            import job
            importlib.reload(job)
            
            mock_generate_datashop.assert_called_once()
            mock_generate_dataset.assert_not_called()

if __name__ == '__main__':
    unittest.main()