""" pycyqle is a Python package that enables model instances
to be created from a relational database and a so-called 'order'
that defines what the resulting models should be composed of.
"""

from functools import reduce
from copy import deepcopy
import importlib
import inspect
import json
from . import utils

__author__ = "Bruno Lange"
__license__ = "MIT"
__version__ = "0.0.1"
__maintainer__ = "Bruno Lange"
__email__ = "blangeram@gmail.com"
__status__ = "Development"

class FluentBase:
    """ Provides simple parameter and dictionary based constructor
    for inheriting classes and fluent setters and getters """

    @classmethod
    def param_build(cls, **kwargs):
        return cls.dict_build(kwargs)

    @classmethod
    def dict_build(cls, dict):
        obj = cls()
        for k, v in dict.items():
            setter = getattr(obj, k)
            setter(v)

        return obj

    def _fluent(self, attr, args):
        if args:
            setattr(self, attr, args[0])
            return self
        return getattr(self, attr)

class Factory(FluentBase):
    """ Factory instances can build models given a data source
    and an order which defines what the final models should be composed
    of.
    """

    # Factories cache
    FACTORIES = {}

    def __init__(self):
        self._model = None
        # key-value mapper for factory components
        self._component_map = {}
        # key-value mapper for factory inventory
        self._inventory_map = {}
        # reference to parent, i.e, the calling parent
        # in the order hierarchy
        self._parent = None
        self._order = None
        self._alias = None
        self._processors = []
        self._filters = []

    def name(self, *args):
        """ Fluent setter/getter for factory name."""
        return self._fluent('_name', args)

    def table(self, *args):
        """ Fluent setter/getter for factory table."""
        return self._fluent('_table', args)

    def alias(self, *args):
        """ Fluent setter/getter for factory table alias."""
        return self._fluent('_alias', args)

    def prefix(self):
        """ Returns factory table prefix."""
        return self.alias() if self.alias() else self.table()

    def primary_key(self, *args):
        """ Fluent setter/getter for factory table primary key."""
        return self._fluent('_primary_key', args)

    def model(self, *args):
        """ Fluent setter/getter for factory model."""
        return self._fluent('_model', args)

    def components(self, *args):
        """ Fluent setter/getter for factory components.
        If no argument is passed, the list of factory components is returned.
        Otherwise, the method takes a list of components as its first argument
        and registers them in the factory component mapper."""
        if not args:
            return self._component_map.values()

        for component in args[0]:
            self._component_map[component.name()] = component

        return self

    def component(self, name):
        """ Returns component associated with given name."""
        return self._component_map[name]

    def has_component(self, name):
        """ Returns True if factory has component in its inventory."""
        return name in self._component_map

    def inventory_items(self, *args):
        """ Fluent setter/getter for factory inventory items."""
        if not args:
            return self._inventory_map

        for inventory in args[0]:
            self._inventory_map[inventory.name()] = inventory

        return self

    def has_inventory_item(self, name):
        """ Return True if inventory associated with name exists within
        the factory."""
        return name in self._inventory_map

    def inventory(self, name):
        """ Returns the inventory item associated with given name."""
        return self._inventory_map[name]

    def parent(self, *args):
        """ Fluent setter/getter for factory parent."""
        if not args:
            return self._parent

        self._parent = {
            'factory': args[0],
            'inventory': args[1]
        }
        return self

    def order(self, *args):
        """ Fluent setter/getter for factory order."""
        if not args:
            return self._order

        self._order = Factory.standardize_order(args[0])
        for component_name in self._order['__components__']:
            if not self.has_component(component_name):
                raise Exception('invalid component [{}]'.format(component_name))
        return self

    def process(self, *args):
        """ If no arguments are passed, returns all processors registered.
        The last argument must be a callable value that takes a model as
        its only argument. Any arguments before the callback set the path
        to the factory which the processor should be attached to.
        """
        if not args:
            return self._processors

        closure = args[-1]
        factory = self._navigate_to_factory(args[:-1])
        factory._process(closure)
        return self

    def _navigate_to_factory(self, path):
        factory = self
        for name in path:
            if not factory.has_inventory_item(name):
                raise Exception('invalid path')

            factory = factory.inventory(name).factory()

        return factory

    def _process(self, closure):
        if not callable(closure):
            raise Exception('processor must be callable')

        self._processors.append(utils.Processor(closure))

    def validate(self):
        """ Returns a list with validation errors.
        An empty list can be interpreted as a 'passing' factory."""
        errors = []
        if not self.name():
            errors.append('missing name')
        if not self.model():
            errors.append('missing model')

        for inventory in self._inventory_map.values():
            errors += inventory.validate()

        return errors

    def model_key(self):
        """ Returns the key associated with the registerd model.
        The key is used in the factory's model map."""
        model = self.model()
        if inspect.isclass(model):
            return model.__name__

        return model

    def build(self, mgr, order, ids):
        """ Returns a list of assembled models given a data source
        and a list of IDs."""
        self.order(order)
        self._model_map = {}
        self._build(mgr, self.order(), Factory.binds(ids), self._model_map)
        if not ids:
            models = self._model_map[self.model()].values()
        else:
            if not isinstance(ids, list):
                ids = [ids]

            models = list(map(
                lambda i: self._model_map[self.model_key()][i],
                filter(lambda i: i in self._model_map[self.model_key()], ids)
            ))

        if ids is None or isinstance(ids, list):
            return models

        if len(models) != 1:
            raise Exception('single build failed')

        return models[0]

    def _build(self, mgr, order, binds, model_map):
        if not order:
            return

        if inspect.isclass(self.model()):
            model_constructor = self.model()
        else:
            module_name, class_name = self.model().rsplit('.', 1)
            model_constructor = getattr(
                importlib.import_module(module_name),
                class_name
            )

        if not self.model_key() in model_map:
            model_map[self.model_key()] = {}

        query = self.query(order['__components__'], binds, 0)
        mgr.execute(query, binds)
        data = mgr.data()

        if not data:
            return

        components = self._get_order_components(order['__components__'])
        payloads = {}
        _map = model_map[self.model_key()]
        for row in data:
            _id = row['__id__']
            if _id in _map:
                model = _map[_id]
            else:
                model = model_constructor(_id)
                _map[_id] = model

            for component in components:
                value = row[component.name()]
                carrier = getattr(model, component.carrier())
                carrier(value)

            for processor in self._processors:
                processor.attach(model)

            if self.parent():
                p_id = row['__pid__']
                if not p_id in payloads:
                    payloads[p_id] = []

                payloads[p_id].append(model)

        del order['__components__']
        for key, components in order.items():
            if not self.has_inventory_item(key):
                raise Exception('inventory item not defined')

            inv = self.inventory(key)
            fac = deepcopy(inv.factory())
            fac.parent(self, inv)
            fac._build(mgr, components, binds, model_map)

        for processor in self._processors:
            processor.run()

        if self.parent():
            parent = self.parent()
            factory = parent['factory']
            inventory = parent['inventory']
            carrier = inventory.carrier()
            parent_map = model_map[factory.model_key()]
            for p_id, models in payloads.items():
                if p_id in parent_map:
                    parent_model = parent_map[p_id]
                    _carrier = getattr(parent_model, carrier)
                    _carrier(models[0] if inventory.single() else models)

    def query(self, components, binds, depth=0):
        query = [
            'SELECT {}'.format(self._compile_select(components)),
            'FROM {}'.format(self._compile_table())
        ]
        if self.parent():
            query.append(self._compile_join())

        query.append('WHERE {}'.format(self._compile_where(binds, depth)))

        tabs = '    '*depth
        return '{}{}'.format(
            tabs,
            '\n{}'.format(tabs).join(query)
        )

    def _compile_select(self, components):
        if components is not None:
            select = [self._column_query('"__id__"')]
        else:
            select = ['DISTINCT {}'.format(self._column_query())]

        if components and self.parent():
            select.append(self.parent()['factory']._column_query('"__pid__"'))

        if components:
            select += list(map(
                lambda c: c.format_column(self.prefix()),
                self._get_order_components(components)
            ))

        return "\n,    ".join(select)

    def _compile_table(self):
        _from = self.table()
        if self.alias():
            _from += ' ' + self.alias()

        return _from

    def _compile_join(self):
        parent = self.parent()
        inventory = parent['inventory']
        join = inventory.join()

        return join.compile()

    def _compile_where(self, binds, depth=0):
        if not binds:
            return '1=1'

        if not self.parent():
            return '{prefix}.{pk} IN ({binds})'.format(
                prefix=self.prefix(),
                pk=self.primary_key() if self.primary_key() else 'ROWID',
                binds=','.join(list(map(
                    lambda i: '%(id{})s'.format(i),
                    range(len(binds))
                )))
            )

        parent_factory = self.parent()['factory']
        return '{table}.{pk} IN (\n{query}\n{depth})'.format(
            table=parent_factory.table(),
            pk=parent_factory.primary_key() if parent_factory.primary_key() else 'ROWID',
            query=parent_factory.query(None, binds, depth+1),
            depth='   '*depth
        )

    def _column_query(self, alias=None):
        if not self.primary_key():
            return 'ROWIDTOCHAR({}.ROWID) AS {}'.format(
                self.prefix(), alias
            )

        return '{prefix}.{pk}{alias}'.format(
            prefix=self.prefix(),
            pk=self.primary_key(),
            alias=' AS {}'.format(alias) if alias else ''
        )

    def _get_order_components(self, names):
        return list(map(
            lambda name: self.component(name),
            names
        ))

    @staticmethod
    def bind_reducer(binds, item):
        index = len(binds)
        binds['id{}'.format(index)] = item
        return binds

    @staticmethod
    def binds(ids):
        if ids is None:
            return []

        if not isinstance(ids, list):
            ids = [ids]

        return reduce(Factory.bind_reducer, ids, {})

    @staticmethod
    def standardize_order(order):
        if not isinstance(order, dict):
            _order = {}
            for i in range(len(order)):
                _order[i] = order[i]
            order = _order

        std_order = {'__components__': []}
        for key, value in order.items():
            if isinstance(key, int):
                std_order['__components__'].append(value)
            else:
                if key == '__components__':
                    std_order[key] = value
                else:
                    std_order[key] = Factory.standardize_order(value)

        return std_order

    @staticmethod
    def from_json(filename):
        with open(filename, 'r') as handle:
            return Factory.from_dict(json.load(handle))

    @staticmethod
    def from_dict(d):
        factory_name = d['name']
        if factory_name in Factory.FACTORIES:
            return Factory.FACTORIES[factory_name]

        if not 'inventory' in d:
            d['inventory'] = {}

        factory = Factory()
        factory.name(factory_name)

        Factory.FACTORIES[factory_name] = factory

        factory\
            .table(d['table'])\
            .primary_key(d['primary_key'])\
            .model(d['model'])\
            .components(Factory.build_components(d['components']))\
            .inventory_items(Factory.build_inventory(d['inventory']))

        if 'alias' in d:
            factory.alias(d['alias'])

        errors = factory.validate()
        if errors:
            raise Exception('invalid factory -> {}'.format(errors))

        return factory

    @staticmethod
    def env_build(factory_name):
        if factory_name in Factory.FACTORIES:
            return Factory.FACTORIES[factory_name]

        factories = Factory.load_factories(factory_name)
        if factory_name not in factories:
            raise Exception('can not load {}'.format(factory_name))

        factory_properties = factories[factory_name]
        if not 'inventory' in factory_properties:
            factory_properties['inventory'] = []

        factory = Factory()
        factory.name(factory_name)

        Factory.FACTORIES[factory_name] = factory

        factory\
            .table(factory_properties['table'])\
            .primary_key(factory_properties['primary_key'])\
            .model(factory_properties['model'])\
            .components(Factory.build_components(factory_properties['components']))\
            .inventory_items(Factory.build_inventory(factory_properties['primary_key']))

        if 'alias' in factory_properties:
            factory.alias(factory_properties['alias'])

        errors = factory.validate()
        if errors:
            raise Exception('invalid factory')

        return factory

    @staticmethod
    def build_components(components_map):
        components = []
        for name, properties in components_map.items():
            component = Component()
            component\
                .name(name)\
                .column(properties['column'])\
                .carrier(properties['carrier'])\
                .ctype(properties['type'] if 'type' in properties else 'string')

            components.append(component)

        return components

    @staticmethod
    def build_inventory(inventory_map):
        inventory_items = []
        for name, properties in inventory_map.items():
            inventory = Inventory()

            inventory\
                .name(name)\
                .factory(Factory.from_json(properties['factory']))\
                .join(Factory.build_join(properties['join']))\
                .carrier(properties['carrier'])\
                .single(properties['single'] if 'single' in properties else False)

            inventory_items.append(inventory)

        return inventory_items

    @staticmethod
    def build_join(properties):
        _join = Join()

        if isinstance(properties, list):
            properties = '\n'.join(_join)

        if isinstance(properties, str):
            return _join.shoehorn(properties)

        return _join\
            .table(properties['table'])\
            .alias(properties['alias'] if 'alias' in properties else None)\
            .on(properties['on'])

    @staticmethod
    def load_factories(factory_name):
        return []

class Component(FluentBase):
    def name(self, *args):
        return self._fluent('_name', args)

    def column(self, *args):
        return self._fluent('_column', args)

    def carrier(self, *args):
        return self._fluent('_carrier', args)

    def ctype(self, *args):
        return self._fluent('_type', args)

    def format_column(self, prefix):
        column = '{}.{}'.format(prefix, self.column())
        return '{} AS {}'.format(column, self.name())

class Inventory(FluentBase):
    def __init__(self):
        super().__init__()
        self._factory = None
        self._inventory_map = {}
        self._single = False

    def inventory(self, *args):
        if not args:
            return self._inventory_map

        items = args[0]
        for inventory in items:
            self._inventory_map[inventory.name()] = inventory

        return self

    def has(self, name):
        return name in self._inventory_map

    def name(self, *args):
        return self._fluent('_name', args)

    def carrier(self, *args):
        return self._fluent('_carrier', args)

    def join(self, *args):
        return self._fluent('_join', args)

    def single(self, *args):
        return self._fluent('_single', args)

    def factory(self, *args):
        if not args:
            return self._factory

        factory = args[0]
        if not isinstance(factory, Factory):
            raise Exception('need Factory object')

        self._factory = factory
        return self

    def validate(self):
        errors = []

        if not self.name():
            errors.append('missing inventory name')
        if not self.factory():
            errors.append('missing inventory factory')
        if not self.join():
            errors.append('missing inventory join')
        if not self.carrier():
            errors.append('missing inventory carrier')

        return errors

class Join(FluentBase):
    def __init__(self):
        super().__init__()
        self._table = None
        self._alias = None
        self._on = None
        self._shoehorn = None

    def table(self, *args):
        return self._fluent('_table', args)

    def alias(self, *args):
        return self._fluent('_alias', args)

    def on(self, *args):
        return self._fluent('_on', args)

    def shoehorn(self, *args):
        return self._fluent('_shoehorn', args)

    def reference(self):
        return self.alias() if self.alias() else self.table()

    def validate(self):
        errors = []
        if not self.table():
            errors.append('missing [table]')
        if not self.on():
            errors.append('missing [on]')

        return errors

    def compile(self, counter_map={}):
        if self.shoehorn():
            return self.shoehorn()

        reference = self.reference()
        if reference in counter_map and counter_map[reference] > 1:
            alias = '{}{}'.format(reference, counter_map[reference])
            replace = alias
        else:
            alias = self.alias()
            replace = reference

        _map = {}
        _map[reference + '.'] = replace + '.'
        return 'JOIN {table}{alias} ON {on}'.format(
            table=self.table(),
            alias=' {}'.format(self.alias()) if self.alias() else '',
            on=str(self.on()).format(_map)
        )
