import unittest
from unittest.mock import Mock, patch, call
import json
from dataset.manifest import build_json_manifest, build_html_manifest
from tests.test_data import SAMPLE_CONTEXT

class TestManifest(unittest.TestCase):

    def setUp(self):
        self.sample_context = SAMPLE_CONTEXT.copy()
        self.mock_s3_client = Mock()
        self.num_chunks = 5
        self.extension = "csv"

    def test_build_json_manifest_structure(self):
        result = build_json_manifest(
            self.mock_s3_client, self.sample_context, 
            self.num_chunks, self.extension
        )
        
        # Verify return value
        expected_key = f"{self.sample_context['job_id']}/manifest.json"
        self.assertEqual(result, expected_key)
        
        # Verify S3 put_object was called
        self.mock_s3_client.put_object.assert_called_once()
        call_args = self.mock_s3_client.put_object.call_args
        
        # Check bucket and key
        self.assertEqual(call_args[1]['Bucket'], self.sample_context['results_bucket_name'])
        self.assertEqual(call_args[1]['Key'], expected_key)
        
        # Parse and verify JSON content
        json_content = json.loads(call_args[1]['Body'])
        self.assertIn('context', json_content)
        self.assertIn('chunks', json_content)
        self.assertEqual(len(json_content['chunks']), self.num_chunks)
        
        # Verify chunk URLs format
        expected_prefix = f"https://{self.sample_context['results_bucket_name']}.s3.us-east-1.amazonaws.com/"
        for i, chunk_url in enumerate(json_content['chunks']):
            expected_url = f"{expected_prefix}{self.sample_context['job_id']}/chunk_{i}.{self.extension}"
            self.assertEqual(chunk_url, expected_url)

    def test_build_json_manifest_xml_extension(self):
        build_json_manifest(
            self.mock_s3_client, self.sample_context, 
            self.num_chunks, "xml"
        )
        
        call_args = self.mock_s3_client.put_object.call_args
        json_content = json.loads(call_args[1]['Body'])
        
        # Verify XML extension in URLs
        for chunk_url in json_content['chunks']:
            self.assertTrue(chunk_url.endswith('.xml'))

    def test_build_json_manifest_context_preservation(self):
        build_json_manifest(
            self.mock_s3_client, self.sample_context, 
            self.num_chunks, self.extension
        )
        
        call_args = self.mock_s3_client.put_object.call_args
        json_content = json.loads(call_args[1]['Body'])
        
        # Verify context is preserved in manifest
        self.assertEqual(json_content['context']['job_id'], self.sample_context['job_id'])
        self.assertEqual(json_content['context']['bucket_name'], self.sample_context['bucket_name'])
        self.assertEqual(json_content['context']['section_ids'], self.sample_context['section_ids'])

    def test_build_json_manifest_zero_chunks(self):
        build_json_manifest(
            self.mock_s3_client, self.sample_context, 
            0, self.extension
        )
        
        call_args = self.mock_s3_client.put_object.call_args
        json_content = json.loads(call_args[1]['Body'])
        
        # Should handle zero chunks gracefully
        self.assertEqual(len(json_content['chunks']), 0)
        self.assertEqual(json_content['chunks'], [])

    def test_build_html_manifest_structure(self):
        result = build_html_manifest(
            self.mock_s3_client, self.sample_context, 
            self.num_chunks, self.extension
        )
        
        # Verify return value
        expected_key = f"{self.sample_context['job_id']}/index.html"
        self.assertEqual(result, expected_key)
        
        # Verify S3 put_object was called
        self.mock_s3_client.put_object.assert_called_once()
        call_args = self.mock_s3_client.put_object.call_args
        
        # Check bucket and key
        self.assertEqual(call_args[1]['Bucket'], self.sample_context['results_bucket_name'])
        self.assertEqual(call_args[1]['Key'], expected_key)
        
        # Verify HTML content
        html_content = call_args[1]['Body']
        self.assertIn('<!doctype html>', html_content)
        self.assertIn('<title>Job Manifest</title>', html_content)
        self.assertIn('<h1>Job Manifest</h1>', html_content)

    def test_build_html_manifest_content_structure(self):
        build_html_manifest(
            self.mock_s3_client, self.sample_context, 
            self.num_chunks, self.extension
        )
        
        call_args = self.mock_s3_client.put_object.call_args
        html_content = call_args[1]['Body']
        
        # Verify configuration table is present
        self.assertIn('<table>', html_content)
        self.assertIn('job_id', html_content)
        self.assertIn(self.sample_context['job_id'], html_content)
        
        # Verify file links are present
        self.assertIn('<ul>', html_content)
        self.assertIn('<li><a href=', html_content)
        
        # Count number of file links
        link_count = html_content.count('<li><a href=')
        self.assertEqual(link_count, self.num_chunks)

    def test_build_html_manifest_url_format(self):
        build_html_manifest(
            self.mock_s3_client, self.sample_context, 
            self.num_chunks, self.extension
        )
        
        call_args = self.mock_s3_client.put_object.call_args
        html_content = call_args[1]['Body']
        
        # Verify URL format in HTML links
        expected_prefix = f"https://{self.sample_context['results_bucket_name']}.s3.us-east-1.amazonaws.com/"
        for i in range(self.num_chunks):
            expected_url = f"{expected_prefix}{self.sample_context['job_id']}/chunk_{i}.{self.extension}"
            self.assertIn(expected_url, html_content)

    def test_build_html_manifest_configuration_display(self):
        build_html_manifest(
            self.mock_s3_client, self.sample_context, 
            self.num_chunks, self.extension
        )
        
        call_args = self.mock_s3_client.put_object.call_args
        html_content = call_args[1]['Body']
        
        # Verify key configuration items are displayed
        for key, value in self.sample_context.items():
            self.assertIn(str(key), html_content)
            self.assertIn(str(value), html_content)

    def test_build_html_manifest_valid_html(self):
        build_html_manifest(
            self.mock_s3_client, self.sample_context, 
            self.num_chunks, self.extension
        )
        
        call_args = self.mock_s3_client.put_object.call_args
        html_content = call_args[1]['Body']
        
        # Basic HTML validation
        self.assertTrue(html_content.startswith('<!doctype html>'))
        self.assertTrue(html_content.endswith('</body></html>'))
        self.assertIn('<html dir="ltr" lang="en">', html_content)
        self.assertIn('<head>', html_content)
        self.assertIn('</head>', html_content)
        self.assertIn('<body>', html_content)

    def test_build_html_manifest_zero_chunks(self):
        build_html_manifest(
            self.mock_s3_client, self.sample_context, 
            0, self.extension
        )
        
        call_args = self.mock_s3_client.put_object.call_args
        html_content = call_args[1]['Body']
        
        # Should still have valid HTML structure with empty list
        self.assertIn('<ul>', html_content)
        self.assertIn('</ul>', html_content)
        self.assertEqual(html_content.count('<li><a href='), 0)

    def test_build_html_manifest_special_characters_in_context(self):
        context_with_special_chars = self.sample_context.copy()
        context_with_special_chars['special_field'] = 'Value with <script>alert("test")</script>'
        
        build_html_manifest(
            self.mock_s3_client, context_with_special_chars, 
            self.num_chunks, self.extension
        )
        
        call_args = self.mock_s3_client.put_object.call_args
        html_content = call_args[1]['Body']
        
        # Verify content is included (basic escaping test)
        self.assertIn('special_field', html_content)
        # Note: The current implementation doesn't escape HTML, which could be a security issue
        # This test documents the current behavior

if __name__ == '__main__':
    unittest.main()