
from collections import OrderedDict, namedtuple

from mimesis.schema import Schema


Column = namedtuple('Column', ['name', 'rng', 'type'])
Dependency = namedtuple('Dependency', ['name', 'scaler'])

class BaseObject:
    TABLE_NAME = None
    BYPASS = False

    def __init__(self, raw):
        if not isinstance(raw, OrderedDict):
            raise TypeError('raw must be a OrderedDict')

        self.raw = raw

    def __getitem__(self, key):
        return self.raw[key]

    def __setitem__(self, key, value):
        self.raw[key] = value

    @classmethod
    def schema(cls, rand_gen):
        pass

    @classmethod
    def _generate_column(cls, rand_gen, column_gen, data_store):
        if column_gen.gen.startswith('choose_from_list'):
            data_path = column_gen.gen.split(' ')[1]
            data = data_store[data_path]
            return rand_gen.choose_from_list(data)

        return getattr(rand_gen, column_gen.gen)(*column_gen.args)

    @classmethod
    def schema_from_source(cls, rand_gen, source, data_store):
        return lambda: OrderedDict([
            (column_name, cls._generate_column(rand_gen, column_gen, data_store))
            for column_name, column_gen in source.items()
            if column_gen.gen != 'skip'
        ])

    @classmethod
    def run_sampling(cls, schema, num_rows):
        objects = Schema(schema).create(iterations=num_rows)
        return [cls(raw) for raw in objects]

    @classmethod
    def sample(cls, rand_gen, num_rows):
        schema = cls.schema(rand_gen)
        return {
            cls.TABLE_NAME: cls.run_sampling(schema, num_rows)
        }

    @classmethod
    def sample_from_source(cls, rand_gen, num_rows, source, data_store) -> list:
        schema = cls.schema_from_source(rand_gen, source, data_store)
        return cls.run_sampling(schema, num_rows)

    def to_sql(self):
        def to_str(value):
            if value is None:
                return ''

            return str(value)

        return '|'.join([to_str(value) for value in (self.raw.values())])

    def set(self, key, value):
        self.raw[key] = value

    def get(self, key):
        return self.raw.get(key)

    def hash(self):
        return hash(frozenset(self.raw.items()))

    def clone(self):
        new_raw = self.__class__(OrderedDict())
        for key, value in self.raw.items():
            new_raw[key] = value

        return new_raw
