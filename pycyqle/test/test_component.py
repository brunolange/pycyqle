import unittest
from pycyqle.factory import Component

class ComponentTest(unittest.TestCase):

    def test_param_build(self):
        component = Component.param_build(
            name='tire',
            carrier='set_tire'
        )
        self._assert_tire_component(component)
        return component

    def test_dict_build(self):
        component = Component.dict_build({
            'name': 'tire',
            'carrier': 'set_tire'
        })
        self._assert_tire_component(component)
        return component

    def _assert_tire_component(self, component):
        self.assertEqual(component.name(), 'tire')
        self.assertEqual(component.carrier(), 'set_tire')

if __name__ == '__main__':
    unittest.main()