"""
This module contains schema-related type definitions.
"""

from collections import namedtuple, OrderedDict
from typing import Any, List, Set, Tuple, Type

from mimesis.schema import Schema

import lib.schema_parser as schema_parser

from lib.data_wrapper import DataWrapper
from lib.random import Random


Column = namedtuple('Column', ['name', 'rng', 'type'])


class Table:
    """Postgres table abstraction"""
    def __init__(self, name, schema_path=None, scaler=1.0):
        self.name = name

        assert schema_path, 'schema_path cannot be None'
        self.schema_path = schema_path
        self.scaler = scaler
        self.schema = schema_parser.Schema(self.schema_path).parse_create_table()

    def get_column_dependencies(self) -> Set[Tuple[str, str]]:
        """Return a set of (table, column) referenced by 'choose_from_list'"""
        deps = set()
        for column_gen in self.schema.values():
            if column_gen.gen.startswith('choose_from_list'):
                path = column_gen.gen.split(' ')[1]
                table, _, column = path.rpartition('.')
                deps.add((table, column))

        return deps

    def generate_columns(self, rand_gen):
        """Read source input and convert it into a mimesis compatible schema."""
        return lambda: OrderedDict([
            (column_name, column.generate(rand_gen))
            for column_name, column in self.schema.items()
            if not column.do_skip
        ])

    @classmethod
    def run_sampling(cls, table_name, schema, num_rows, rand_gen,
                     linked_columns, cache) -> List:
        """Sample num_rows on the provided schema."""
        objects = Schema(schema).create(iterations=num_rows)
        data = [cls(raw) for raw in objects]
        cache.add(table_name, data)

        # 2nd pass to satisfy linked columns, either to self or another table
        for column_name, column_gen in linked_columns:
            data_path = column_gen.gen.split(' ')[1]
            cache_entry = cache.retrieve(data_path)
            for row in data:
                row[column_name] = rand_gen.choose_from_list(cache_entry)

        return data

    def generate_data(self, rand_gen, num_rows, cache) -> List:
    # def sample_from_source(cls, rand_gen, num_rows, source, table_name, cache) -> List:
        """Sample num_rows on the provided source."""
        columns = self.generate_columns(rand_gen)
        linked_columns = [
            (column_name, column) for column_name, column
            in self.schema.items() if column.is_linked
        ]

        objects = Schema(columns).create(iterations=num_rows)
        data = [DataWrapper(object) for object in objects]
        cache.add(self.name, data)

        # 2nd pass to satisfy linked columns, either to self or another table
        for column_name, column in linked_columns:
            data_path = column.gen.split(' ')[1]
            cache_entry = cache.retrieve(data_path)
            for row in data:
                row[column_name] = rand_gen.choose_from_list(cache_entry)

        return data

        # return cls.run_sampling(
        #     table_name, schema, num_rows, rand_gen, linked_columns, cache)
