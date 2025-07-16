import unittest
from dataset.datashop import to_xml_message, trim_to_100_bytes
from dataset.lookup import post_process

import json

class TestDatashop(unittest.TestCase):

    def test_trim_to_100_bytes(self):
        self.assertEqual(trim_to_100_bytes("1234567890" * 10), "1234567890" * 10)
        self.assertEqual(trim_to_100_bytes("1234567890" * 10 + "1234567890"), "1234567890" * 10)

    def test_from_part_attempt(self):

        # read the test.json file from this dir
        with open('tests/test.json') as f:
            data1 = json.load(f)

        with open('tests/attempt2.json') as f:
            data2 = json.load(f)

        # create a fake context
        context = {
            'dataset_name': 'test_dataset',
            'skill_titles': {
                '161568': 'skill text 1'
            },
            'hierarchy': {
                '152914': {'graded': True, 'title': 'Assessment 1'},
                '24': {'title': 'Unit 1', 'children': [25]},
                '25': {'title': 'Module 1', 'children': [152914]},
            },
            'anonymize': True,
            'activities': {
                '162143': {
                    'choices': [
                        {'id': '1040950542', 'content': [{'text': 'choice A'}]},
                        {'id': '10', 'text': 'choice B'},
                        {'id': '0542', 'text': 'choice C'}
                    ],
                    'type': 'oli_multiple_choice',
                    'parts': [
                        {
                            'id': '1', 
                            'hints': [
                                {'id': 'h1', 'content': [{'text': 'Hint text 1'}]},
                                {'id': 'h2', 'content': [{'text': 'Hint text 2'}]},
                            ]
                        }
                    ]
                }
            }
        }
        
        post_process(context)
        # call the from_part_attempt function with the data
        result1 = to_xml_message(data1, context)
        result2 = to_xml_message(data2, context)
        
         # write xml to file
        with open('tests/output.xml', 'w') as f:
            f.write('<?xml version= \"1.0\" encoding= \"UTF-8\"?>\n')
            f.write('<tutor_related_message_sequence version_number= \"4\" xmlns:xsi= \"http://www.w3.org/2001/XMLSchema-instance\" xsi:noNamespaceSchemaLocation= \"http://pslcdatashop.org/dtd/tutor_message_v4.xsd\">\n')
            f.write(result1)
            f.write(result2)
            f.write('\n</tutor_related_message_sequence>')

if __name__ == '__main__':
    unittest.main()
