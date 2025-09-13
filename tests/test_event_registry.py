import unittest
from dataset.event_registry import get_event_config, registered_events, attempts_columns, page_viewed_columns, video_columns

class TestEventRegistry(unittest.TestCase):

    def test_get_event_config_attempt_evaluated(self):
        handler, columns = get_event_config("attempt_evaluated")
        
        # Verify handler is callable
        self.assertTrue(callable(handler))
        
        # Verify columns match expected structure
        self.assertEqual(columns, attempts_columns)
        self.assertIn("event_type", columns)
        self.assertIn("timestamp", columns)
        self.assertIn("user_id", columns)
        self.assertIn("section_id", columns)

    def test_get_event_config_page_viewed(self):
        handler, columns = get_event_config("page_viewed")
        
        self.assertTrue(callable(handler))
        self.assertEqual(columns, page_viewed_columns)
        self.assertIn("event_type", columns)
        self.assertIn("timestamp", columns)
        self.assertIn("page_id", columns)

    def test_get_event_config_video(self):
        handler, columns = get_event_config("video")
        
        self.assertTrue(callable(handler))
        self.assertEqual(columns, video_columns)
        self.assertIn("video_url", columns)
        self.assertIn("video_title", columns)
        self.assertIn("video_length", columns)

    def test_get_event_config_unknown_event(self):
        with self.assertRaises(KeyError):
            get_event_config("unknown_event_type")

    def test_registered_events_structure(self):
        # Verify all registered events have correct structure
        for event_name, (handler, columns) in registered_events.items():
            self.assertTrue(callable(handler), f"Handler for {event_name} is not callable")
            self.assertIsInstance(columns, list, f"Columns for {event_name} is not a list")
            self.assertTrue(len(columns) > 0, f"Columns for {event_name} is empty")

    def test_attempts_columns_content(self):
        required_fields = [
            "event_type", "timestamp", "user_id", "section_id", "project_id",
            "publication_id", "page_id", "activity_id", "part_id", "score", "out_of"
        ]
        
        for field in required_fields:
            self.assertIn(field, attempts_columns, f"Missing required field: {field}")

    def test_page_viewed_columns_content(self):
        required_fields = [
            "event_type", "timestamp", "user_id", "section_id", 
            "project_id", "publication_id", "page_id"
        ]
        
        for field in required_fields:
            self.assertIn(field, page_viewed_columns, f"Missing required field: {field}")

    def test_video_columns_content(self):
        required_fields = [
            "event_type", "timestamp", "user_id", "section_id", "project_id",
            "publication_id", "page_id", "video_url", "video_title"
        ]
        
        for field in required_fields:
            self.assertIn(field, video_columns, f"Missing required field: {field}")

    def test_columns_uniqueness(self):
        # Test that each column list has unique elements
        self.assertEqual(len(attempts_columns), len(set(attempts_columns)), 
                        "attempts_columns contains duplicates")
        self.assertEqual(len(page_viewed_columns), len(set(page_viewed_columns)), 
                        "page_viewed_columns contains duplicates")
        self.assertEqual(len(video_columns), len(set(video_columns)), 
                        "video_columns contains duplicates")

if __name__ == '__main__':
    unittest.main()