import unittest
from pycyqle.factory import Factory

class FactoryTest(unittest.TestCase):
    def setUp(self):
        self.factory = Factory()
        self.factory\
            .name('test-factory')\
            .table('test-table')\
            .primary_key('id')

    def test_factory_build(self):
        self.assertEqual(self.factory.name(), 'test-factory')
        self.assertEqual(self.factory.table(), 'test-table')
        self.assertEqual(self.factory.primary_key(), 'id')

if __name__ == '__main__':
    unittest.main()