import unittest
from pycyqle.factory import Factory

class FactoryTest(unittest.TestCase):

    def test_param_build(self):
        factory = Factory.param_build(
            name='bicycle-factory',
            table='bicycle',
            primary_key='id'
        )
        self._assert_bicycle_factory(factory)
        return factory

    def test_dict_build(self):
        factory = Factory.dict_build({
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

if __name__ == '__main__':
    unittest.main()
