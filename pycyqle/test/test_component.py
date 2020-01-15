import unittest

from pycyqle.builder import dict_build, param_build
from pycyqle.factory import Component


class ComponentTest(unittest.TestCase):

    def test_param_build(self):
        component = param_build(
            Component,
            name='tire',
            carrier='set_tire'
        )
        self._assert_tire_component(component)
        return component

    def test_dict_build(self):
        component = dict_build(Component, {
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
