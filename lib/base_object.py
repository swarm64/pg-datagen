
from collections import OrderedDict, namedtuple

from mimesis.schema import Schema


Column = namedtuple('Column', ['name', 'rng', 'type'])


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
    def run_sampling(cls, schema, num_rows):
        objects = Schema(schema).create(iterations=num_rows)
        return [cls(raw) for raw in objects]

    @classmethod
    def sample(cls, rand_gen, num_rows):
        schema = cls.schema(rand_gen)
        return {
            cls.TABLE_NAME: cls.run_sampling(schema, num_rows)
        }

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
