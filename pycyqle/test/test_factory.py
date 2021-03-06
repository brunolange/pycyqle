import re
import unittest
from functools import partial

from pycyqle.builder import dict_build, param_build
from pycyqle.factory import Component, Factory


class FactoryTest(unittest.TestCase):

    def test_param_build(self):
        factory = param_build(
            Factory,
            name='bicycle-factory',
            table='bicycle',
            primary_key='id'
        )
        self._assert_bicycle_factory(factory)
        return factory

    def test_dict_build(self):
        factory = dict_build(Factory, {
            'name': 'bicycle-factory',
            'table': 'bicycle',
            'primary_key': 'id'
        })
        self._assert_bicycle_factory(factory)
        return factory

    def _assert_bicycle_factory(self, factory):
        self.assertEqual(factory.name(), 'bicycle-factory')
        self.assertEqual(factory.table(), 'bicycle')
        self.assertEqual(factory.primary_key(), 'id')

    @staticmethod
    def _format_query(query):
        return re.sub(r'\s?,\s?', ',', ' '.join(query.split()))

    def test_query(self):
        components = list(map(partial(dict_build, Component), [
            {'name': 'tire', 'column': 'tire'},
            {'name': 'seat', 'column': 'seat'}
        ]))
        factory = self.test_dict_build()
        factory.components(components)

        self.assertEqual(len(factory.components()), len(components))
        self.assertEqual(
            FactoryTest._format_query(factory.query(['tire'], {})),
            FactoryTest._format_query("""
                SELECT bicycle.id AS "__id__"
                ,   bicycle.tire AS tire
                FROM bicycle WHERE 1=1
            """)
        )

        new_components = list(map(partial(dict_build, Component), [
            {'name': 'pedal', 'column': 'pedal'}
        ]))
        factory.components(components + new_components)

        self.assertEqual(
            len(factory.components()),
            len(components) + len(new_components)
        )
        self.assertEqual(
            FactoryTest._format_query(
                factory.query(['seat', 'pedal'], {
                    'id0': 42
                })
            ),
            FactoryTest._format_query("""
            SELECT bicycle.id AS "__id__"
            ,   bicycle.seat AS seat
            ,   bicycle.pedal AS pedal
            FROM bicycle WHERE bicycle.id IN (%(id0)s)
            """)
        )


if __name__ == '__main__':
    unittest.main()
