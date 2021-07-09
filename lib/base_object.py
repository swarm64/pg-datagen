"""
Base class supplying core methods for sampled random objects.
"""

from collections import OrderedDict
from typing import Any, List, Type

from mimesis.schema import Schema

from lib.random import Random


class BaseObject:
    """Class to mainly inherit from and wrap raw data into."""

    def __init__(self, raw: Type[OrderedDict]):
        if not isinstance(raw, OrderedDict):
            raise TypeError('raw must be a OrderedDict')

        self.raw = raw

    def __getitem__(self, key):
        return self.raw[key]

    def __setitem__(self, key, value):
        self.raw[key] = value

    def __eq__(self, other):
        return self.raw == other.raw

    @classmethod
    def _generate_column(cls, rand_gen: Type[Random], column_gen, cache) -> Any:
        """
        Generate a column by determining which generator to use. Optionally
        apply information about whether to return NULL on probability basis.
        """
        if column_gen.none_prob:
            if rand_gen.bool_sample(column_gen.none_prob):
                return None

        if column_gen.gen.startswith('choose_from_list'):
            data_path = column_gen.gen.split(' ')[1]
            return rand_gen.choose_from_list(cache.retrieve(data_path))

        return getattr(rand_gen, column_gen.gen)(*column_gen.args)

    @classmethod
    def schema_from_source(cls, rand_gen, source, cache):
        """Read source input and convert it into a mimesis compatible schema."""
        return lambda: OrderedDict([
            (column_name, cls._generate_column(rand_gen, column_gen, cache))
            for column_name, column_gen in source.items()
            if column_gen.gen != 'skip'
        ])

    @classmethod
    def run_sampling(cls, schema, num_rows):
        """Sample num_rows on the provided schema."""
        objects = Schema(schema).create(iterations=num_rows)
        return [cls(raw) for raw in objects]

    @classmethod
    def sample_from_source(cls, rand_gen, num_rows, source, cache) -> List:
        """Sample num_rows on the provided source."""
        schema = cls.schema_from_source(rand_gen, source, cache)
        return cls.run_sampling(schema, num_rows)

    def to_sql(self) -> str:
        """Convert this object to an SQL string.
        """
        def to_str(value):
            if value is None:
                return ''

            return str(value)

        return '|'.join([to_str(value) for value in self.raw.values()])

    def set(self, key: str, value: Any):
        """Set the value of an entry in the underlying map."""
        self.raw[key] = value

    def get(self, key: str) -> Any:
        """Retrieve a value from the underlying map."""
        return self.raw.get(key)
