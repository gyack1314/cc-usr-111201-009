"""核对领域原语的业务数值。"""
import unittest
from analysis_data.io import load
from analysis_data.domain import summarize
class DomainTests(unittest.TestCase):
    def test_request_inventory(self):
        r=summarize(load('data/normal.json')[0]);self.assertEqual(r['request_count'],8);self.assertEqual(r['types'],{'image':4,'text':4})
        self.assertEqual(r['last_arrival'],8)
        empty=summarize(load('data/empty.json')[0]);self.assertEqual(empty['request_count'],0);self.assertIsNone(empty['first_arrival'])
