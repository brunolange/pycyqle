import unittest
from pycyqle.factory import Factory, Component
from . import utils

class FilterTest(unittest.TestCase):
    def test_filter(self):
        factory = Factory.param_build(
            name='bicycle-factory',
            table='bicycle',
            primary_key='id'
        )
        factory.components([
            Component.dict_build(c) for c in [
                {'name': 'tire', 'column': 'tire'},
                {'name': 'seat', 'column': 'seat'}
            ]
        ])
        factory.filter("bicycle.tire LIKE 'michelin'")
        self.assertEqual(
            utils.format_query(factory.query(['tire'], {})),
            utils.format_query("""
                SELECT bicycle.id AS "__id__"
                ,   bicycle.tire AS tire
                FROM bicycle
                WHERE 1=1
                AND bicycle.tire LIKE 'michelin'
            """
        ))
