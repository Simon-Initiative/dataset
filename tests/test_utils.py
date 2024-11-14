import unittest
from dataset.utils import encode_array, encode_json

class TestUtils(unittest.TestCase):

    def test_encode_array(self):
        self.assertEqual(encode_array([1, 2, 3]), '"1,2,3"')
        self.assertEqual(encode_array([]), '""')
        self.assertEqual(encode_array([42]), '"42"')
        self.assertEqual(encode_array([-1, -2, -3]), '"-1,-2,-3"')
        self.assertEqual(encode_array([10, -20, 30]), '"10,-20,30"')

    def test_encode_json(self):
        self.assertEqual(encode_json({"key": "value"}), '"{"key":"value"}"')
        self.assertEqual(encode_json({"a": 1, "b": 2}), '"{"a":1,"b":2}"')
        self.assertEqual(encode_json({"nested": {"key": "value"}}), '"{"nested":{"key":"value"}}"')
        self.assertEqual(encode_json([1, 2, 3]), '"[1,2,3]"')
        self.assertEqual(encode_json({}), '"{}"')
        self.assertEqual(encode_json([]), '"[]"')
        self.assertEqual(encode_json({"key": "value\nwith\nnewlines"}), '"{"key":"value\\nwith\\nnewlines"}"')
        self.assertEqual(encode_json({"key": 'value with "quotes"'}), '"{"key":"value with \\"quotes\\""}"')

if __name__ == '__main__':
    unittest.main()
