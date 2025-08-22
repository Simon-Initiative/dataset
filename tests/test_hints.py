import unittest
from dataset.datashop import to_xml_message
import json


class TestDatashop(unittest.TestCase):

    

    def test_hints(self):

        # read the test.json file from this dir
        with open('tests/1299.json') as f:
            context = json.load(f)

        with open('tests/evaluated.json') as f:
            data = json.load(f)

        xml  = (to_xml_message(data, context))

        # write xml to file
        with open('tests/1299.xml', 'w') as f:
            f.write(xml)


if __name__ == '__main__':
    unittest.main()
