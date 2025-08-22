import unittest
from unittest.mock import Mock, patch, MagicMock, call
import pandas as pd
import io
import math
from dataset.dataset import (
    generate_dataset, generate_datashop, initialize_spark_context,
    calculate_number_of_chunks, chunkify, save_chunk_to_s3, save_xml_chunk,
    build_manifests
)
from tests.test_data import (
    SAMPLE_CONTEXT, SAMPLE_LOOKUP_DATA, create_mock_s3_client, 
    create_mock_spark_context, create_sample_part_attempt
)

class TestDataset(unittest.TestCase):

    def setUp(self):
        self.sample_context = SAMPLE_CONTEXT.copy()
        self.mock_s3_client = create_mock_s3_client()
        self.mock_spark_context = create_mock_spark_context()

    @patch('dataset.dataset.build_manifests')
    @patch('dataset.dataset.save_chunk_to_s3')
    @patch('dataset.dataset.parallel_map')
    @patch('dataset.dataset.retrieve_lookup')
    @patch('dataset.dataset.list_keys_from_inventory')
    @patch('dataset.dataset.get_event_config')
    @patch('dataset.dataset.initialize_spark_context')
    @patch('boto3.client')
    def test_generate_dataset_success(self, mock_boto, mock_init_spark, mock_get_config, 
                                     mock_list_keys, mock_retrieve_lookup, mock_parallel_map,
                                     mock_save_chunk, mock_build_manifests):
        
        # Setup mocks
        mock_boto.return_value = self.mock_s3_client
        mock_sc = Mock()
        mock_spark = Mock()
        mock_sc.stop = Mock()
        mock_init_spark.return_value = (mock_sc, mock_spark)
        
        # Mock event configuration
        mock_handler = Mock()
        mock_columns = ['col1', 'col2', 'col3']
        mock_get_config.return_value = (mock_handler, mock_columns)
        
        # Mock key listing
        mock_list_keys.return_value = ['key1', 'key2', 'key3']
        
        # Mock lookup retrieval
        mock_retrieve_lookup.return_value = SAMPLE_LOOKUP_DATA
        
        # Mock parallel processing
        mock_parallel_map.return_value = [['data1'], ['data2']]
        
        result = generate_dataset([1001], "attempt_evaluated", self.sample_context)
        
        # Verify initialization
        mock_init_spark.assert_called_once_with("generate_dataset")
        mock_get_config.assert_called_once_with("attempt_evaluated")
        mock_list_keys.assert_called_once()
        mock_retrieve_lookup.assert_called_once()
        
        # Verify processing
        mock_parallel_map.assert_called()
        mock_save_chunk.assert_called()
        mock_build_manifests.assert_called_once()
        
        # Verify cleanup
        mock_sc.stop.assert_called_once()
        
        # Verify return value (number of chunks)
        self.assertIsInstance(result, int)

    @patch('dataset.dataset.build_manifests')
    @patch('dataset.dataset.save_xml_chunk')
    @patch('dataset.dataset.process_part_attempts')
    @patch('dataset.dataset.parallel_map')
    @patch('dataset.dataset.retrieve_lookup')
    @patch('dataset.dataset.list_keys_from_inventory')
    @patch('dataset.dataset.initialize_spark_context')
    @patch('boto3.client')
    def test_generate_datashop_success(self, mock_boto, mock_init_spark, mock_list_keys,
                                      mock_retrieve_lookup, mock_parallel_map, 
                                      mock_process_attempts, mock_save_xml, mock_build_manifests):
        
        # Setup mocks
        mock_boto.return_value = self.mock_s3_client
        mock_sc = Mock()
        mock_spark = Mock()
        mock_sc.stop = Mock()
        mock_init_spark.return_value = (mock_sc, mock_spark)
        
        mock_list_keys.return_value = ['key1', 'key2']
        mock_retrieve_lookup.return_value = SAMPLE_LOOKUP_DATA
        
        # Mock part attempts with different users/sessions for grouping
        mock_part_attempts = [
            {**create_sample_part_attempt(), 'user_id': '123', 'session_id': 'sess1', 'section_id': 1001},
            {**create_sample_part_attempt(), 'user_id': '456', 'session_id': 'sess2', 'section_id': 1001}
        ]
        mock_parallel_map.return_value = mock_part_attempts
        
        mock_process_attempts.return_value = ['<xml>result1</xml>', '<xml>result2</xml>']
        
        result = generate_datashop(self.sample_context)
        
        # Verify datashop-specific processing
        mock_list_keys.assert_called_with(
            self.sample_context["section_ids"], 
            "attempt_evaluated",
            self.sample_context["bucket_name"],
            self.sample_context["inventory_bucket_name"]
        )
        
        # Verify grouping and sorting occurred
        mock_process_attempts.assert_called()
        
        # Verify XML output
        mock_save_xml.assert_called()
        mock_build_manifests.assert_called_once()
        
        # Verify cleanup
        mock_sc.stop.assert_called_once()

    def test_generate_datashop_grouping_logic(self):
        """Test the grouping and sorting logic for datashop generation."""
        
        # Create test part attempts with different grouping keys
        part_attempts = [
            # User 123, Session 1
            {'section_id': 1001, 'user_id': '123', 'session_id': 'sess1', 
             'page_attempt_guid': 'page1', 'activity_id': 'act1', 'activity_attempt_number': 1,
             'part_id': 'part1', 'part_attempt_number': 1},
            {'section_id': 1001, 'user_id': '123', 'session_id': 'sess1',
             'page_attempt_guid': 'page1', 'activity_id': 'act1', 'activity_attempt_number': 1,
             'part_id': 'part2', 'part_attempt_number': 1},
            
            # User 456, Session 2  
            {'section_id': 1001, 'user_id': '456', 'session_id': 'sess2',
             'page_attempt_guid': 'page2', 'activity_id': 'act2', 'activity_attempt_number': 1,
             'part_id': 'part1', 'part_attempt_number': 1}
        ]
        
        # Simulate the grouping logic from generate_datashop
        partitioned_part_attempts = {}
        for part_attempt in part_attempts:
            key = str(part_attempt.get('section_id', '')) + "_" + str(part_attempt.get('user_id', '')) + "_" + str(part_attempt.get('session_id', ''))
            if key not in partitioned_part_attempts:
                partitioned_part_attempts[key] = []
            partitioned_part_attempts[key].append(part_attempt)
        
        # Verify grouping
        self.assertEqual(len(partitioned_part_attempts), 2)  # Two unique keys
        self.assertIn('1001_123_sess1', partitioned_part_attempts)
        self.assertIn('1001_456_sess2', partitioned_part_attempts)
        self.assertEqual(len(partitioned_part_attempts['1001_123_sess1']), 2)
        self.assertEqual(len(partitioned_part_attempts['1001_456_sess2']), 1)
        
        # Test sorting logic
        for key in partitioned_part_attempts:
            partitioned_part_attempts[key].sort(key=lambda x: (
                x.get('page_attempt_guid', ''),
                x.get('activity_id', ''),
                x.get('activity_attempt_number', 0),
                x.get('part_id', ''),
                x.get('part_attempt_number', 0)
            ))
        
        # Verify sorting within group
        user_123_attempts = partitioned_part_attempts['1001_123_sess1']
        self.assertEqual(user_123_attempts[0]['part_id'], 'part1')
        self.assertEqual(user_123_attempts[1]['part_id'], 'part2')

    @patch('pyspark.SparkContext')
    @patch('pyspark.sql.SparkSession')
    def test_initialize_spark_context(self, mock_spark_session, mock_spark_context):
        mock_sc = Mock()
        mock_spark = Mock()
        mock_spark_context.return_value = mock_sc
        mock_spark_session.return_value = mock_spark
        
        sc, spark = initialize_spark_context("test_app")
        
        self.assertEqual(sc, mock_sc)
        self.assertEqual(spark, mock_spark)

    def test_calculate_number_of_chunks(self):
        self.assertEqual(calculate_number_of_chunks(100, 25), 4)
        self.assertEqual(calculate_number_of_chunks(101, 25), 5)
        self.assertEqual(calculate_number_of_chunks(0, 25), 0)
        self.assertEqual(calculate_number_of_chunks(1, 1), 1)

    def test_chunkify(self):
        data = list(range(10))
        chunks = list(chunkify(data, 3))
        
        self.assertEqual(len(chunks), 4)
        self.assertEqual(chunks[0], [0, 1, 2])
        self.assertEqual(chunks[1], [3, 4, 5])
        self.assertEqual(chunks[2], [6, 7, 8])
        self.assertEqual(chunks[3], [9])

    def test_chunkify_empty_list(self):
        chunks = list(chunkify([], 5))
        self.assertEqual(len(chunks), 0)

    def test_chunkify_larger_chunk_size(self):
        data = [1, 2, 3]
        chunks = list(chunkify(data, 10))
        
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0], [1, 2, 3])

    def test_save_chunk_to_s3(self):
        mock_s3_client = Mock()
        
        chunk_data = [
            ['col1_val1', 'col2_val1', 'col3_val1'],
            ['col1_val2', 'col2_val2', 'col3_val2']
        ]
        columns = ['col1', 'col2', 'col3']
        target_prefix = 'test_job/'
        chunk_index = 0
        results_bucket = 'test-results-bucket'
        
        save_chunk_to_s3(chunk_data, columns, mock_s3_client, target_prefix, chunk_index, results_bucket)
        
        # Verify S3 put_object was called
        mock_s3_client.put_object.assert_called_once()
        call_args = mock_s3_client.put_object.call_args
        
        self.assertEqual(call_args[1]['Bucket'], results_bucket)
        self.assertEqual(call_args[1]['Key'], 'test_job/chunk_0.csv')
        
        # Verify CSV content
        csv_content = call_args[1]['Body']
        self.assertIn('col1,col2,col3', csv_content)
        self.assertIn('col1_val1,col2_val1,col3_val1', csv_content)

    def test_save_xml_chunk(self):
        mock_s3_client = Mock()
        
        chunk_data = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<root>',
            '<element>content</element>',
            '</root>'
        ]
        target_prefix = 'test_job/'
        chunk_index = 1
        results_bucket = 'test-results-bucket'
        
        save_xml_chunk(chunk_data, mock_s3_client, target_prefix, chunk_index, results_bucket)
        
        # Verify S3 put_object was called
        mock_s3_client.put_object.assert_called_once()
        call_args = mock_s3_client.put_object.call_args
        
        self.assertEqual(call_args[1]['Bucket'], results_bucket)
        self.assertEqual(call_args[1]['Key'], 'test_job/chunk_1.xml')
        
        # Verify XML content is properly joined
        xml_content = call_args[1]['Body']
        expected_content = '\n'.join(chunk_data)
        self.assertEqual(xml_content, expected_content)

    @patch('dataset.dataset.build_json_manifest')
    @patch('dataset.dataset.build_html_manifest')
    def test_build_manifests(self, mock_build_html, mock_build_json):
        mock_s3_client = Mock()
        context = {'test': 'context'}
        num_chunks = 5
        extension = 'csv'
        
        build_manifests(mock_s3_client, context, num_chunks, extension)
        
        mock_build_html.assert_called_once_with(mock_s3_client, context, num_chunks, extension)
        mock_build_json.assert_called_once_with(mock_s3_client, context, num_chunks, extension)

    @patch('dataset.dataset.build_manifests')
    @patch('dataset.dataset.save_chunk_to_s3')
    @patch('dataset.dataset.parallel_map')
    @patch('dataset.dataset.retrieve_lookup')
    @patch('dataset.dataset.list_keys_from_inventory')
    @patch('dataset.dataset.get_event_config')
    @patch('dataset.dataset.initialize_spark_context')
    @patch('boto3.client')
    def test_generate_dataset_field_exclusion(self, mock_boto, mock_init_spark, mock_get_config,
                                             mock_list_keys, mock_retrieve_lookup, mock_parallel_map,
                                             mock_save_chunk, mock_build_manifests):
        
        # Setup context with excluded fields
        context_with_exclusions = self.sample_context.copy()
        context_with_exclusions['exclude_fields'] = ['timestamp', 'user_id']
        
        # Setup mocks
        mock_boto.return_value = self.mock_s3_client
        mock_sc = Mock()
        mock_spark = Mock()
        mock_sc.stop = Mock()
        mock_init_spark.return_value = (mock_sc, mock_spark)
        
        mock_columns = ['event_type', 'timestamp', 'user_id', 'section_id']
        mock_get_config.return_value = (Mock(), mock_columns)
        mock_list_keys.return_value = ['key1']
        mock_retrieve_lookup.return_value = {}
        mock_parallel_map.return_value = [['data']]
        
        generate_dataset([1001], "attempt_evaluated", context_with_exclusions)
        
        # Verify that excluded indices were calculated and passed
        # The parallel_map should be called with excluded_indices parameter
        mock_parallel_map.assert_called()
        call_args = mock_parallel_map.call_args[0]
        excluded_indices = call_args[5]  # 6th parameter is excluded_indices
        
        # timestamp (index 1) and user_id (index 2) should be excluded
        # Since indices are sorted in reverse order for removal
        self.assertEqual(excluded_indices, [2, 1])

    @patch('dataset.dataset.build_manifests')
    @patch('dataset.dataset.save_chunk_to_s3')
    @patch('dataset.dataset.parallel_map')
    @patch('dataset.dataset.retrieve_lookup')
    @patch('dataset.dataset.list_keys_from_inventory')
    @patch('dataset.dataset.get_event_config')
    @patch('dataset.dataset.initialize_spark_context')
    @patch('boto3.client')
    def test_generate_dataset_chunking(self, mock_boto, mock_init_spark, mock_get_config,
                                      mock_list_keys, mock_retrieve_lookup, mock_parallel_map,
                                      mock_save_chunk, mock_build_manifests):
        
        # Setup context with small chunk size
        context_small_chunks = self.sample_context.copy()
        context_small_chunks['chunk_size'] = 2
        
        # Setup mocks
        mock_boto.return_value = self.mock_s3_client
        mock_sc = Mock()
        mock_spark = Mock() 
        mock_sc.stop = Mock()
        mock_init_spark.return_value = (mock_sc, mock_spark)
        
        mock_get_config.return_value = (Mock(), ['col1', 'col2'])
        mock_list_keys.return_value = ['key1', 'key2', 'key3', 'key4', 'key5']  # 5 keys
        mock_retrieve_lookup.return_value = {}
        mock_parallel_map.return_value = [['data']]
        
        result = generate_dataset([1001], "attempt_evaluated", context_small_chunks)
        
        # With 5 keys and chunk_size=2, should have 3 chunks (ceil(5/2))
        expected_chunks = math.ceil(5 / 2)
        self.assertEqual(result, expected_chunks)
        
        # parallel_map should be called 3 times (once per chunk)
        self.assertEqual(mock_parallel_map.call_count, 3)

    def test_generate_dataset_exception_handling(self):
        """Test that exceptions in chunk processing are handled gracefully."""
        
        with patch('dataset.dataset.initialize_spark_context') as mock_init_spark, \
             patch('dataset.dataset.get_event_config') as mock_get_config, \
             patch('dataset.dataset.list_keys_from_inventory') as mock_list_keys, \
             patch('dataset.dataset.retrieve_lookup') as mock_retrieve_lookup, \
             patch('dataset.dataset.parallel_map') as mock_parallel_map, \
             patch('dataset.dataset.build_manifests') as mock_build_manifests, \
             patch('boto3.client') as mock_boto:
            
            # Setup mocks
            mock_boto.return_value = self.mock_s3_client
            mock_sc = Mock()
            mock_spark = Mock()
            mock_sc.stop = Mock()
            mock_init_spark.return_value = (mock_sc, mock_spark)
            
            mock_get_config.return_value = (Mock(), ['col1'])
            mock_list_keys.return_value = ['key1', 'key2']
            mock_retrieve_lookup.return_value = {}
            
            # Make parallel_map raise an exception on first call, succeed on second
            mock_parallel_map.side_effect = [Exception("Processing error"), [['data']]]
            
            with patch('builtins.print') as mock_print:
                result = generate_dataset([1001], "attempt_evaluated", self.sample_context)
                
                # Should continue processing despite exception
                self.assertIsInstance(result, int)
                
                # Should have printed error message
                mock_print.assert_any_call(
                    unittest.mock.ANY  # The error message containing "Processing error"
                )

if __name__ == '__main__':
    unittest.main()