import unittest
from dataset.datashop import to_xml_message, calculate_ancestors
import json

class TestDatashop(unittest.TestCase):

    def test_from_part_attempt(self):

        # read the test.json file from this dir
        with open('tests/test.json') as f:
            data = json.load(f)

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
            'activities': {
                '162143': {
                    'choices': [
                        {'id': '1040950542', 'content': [{'text': 'choice A'}]},
                        {'id': '10', 'text': 'choice B'},
                        {'id': '0542', 'text': 'choice C'}
                    ],
                    'type': 'oli_multiple_choice',
                    'parts': {
                        '1': {
                            'hints': [
                                {'id': 'h1', 'content': [{'text': 'Hint text 1'}]},
                                {'id': 'h2', 'content': [{'text': 'Hint text 2'}]},
                            ]
                        }
                    }
                }
            }
        }
        print("here")
        calculate_ancestors(context)
        print(context['hierarchy'])
        # call the from_part_attempt function with the data
        result = to_xml_message(data, context)
        print(result)


if __name__ == '__main__':
    unittest.main()
