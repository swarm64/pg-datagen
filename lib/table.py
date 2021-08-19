"""
This module contains schema-related type definitions.
"""

from collections import namedtuple, OrderedDict
from typing import Any, List, Set, Tuple, Type

from loguru import logger
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

    def __repr__(self):
        return f'Table { self.name }'

    def get_column_dependencies(self) -> Set[Tuple[str, str]]:
        """Return a set of (<path>, <path>) referenced by 'choose_from_list'"""
        deps = set()
        for column, column_gen in self.schema.items():
            if column_gen.gen.startswith('choose_from_list'):
                source = column_gen.gen.split(' ')[1]
                target = f'{ self.name }.{ column }'

                if source.startswith(self.name):
                    logger.info(f'Self-Dep: { source }')
                    deps.add((source, None))

                else:
                    logger.info(f'Dep: { source } -> { target }')
                    deps.add((source, target))

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
            logger.debug(f'Retrieving data from cache for: { data_path }')

            cache_entry = cache.retrieve(data_path)
            assert cache_entry, f'Cache entry for { data_path } is empty.'

            for row in data:
                row[column_name] = rand_gen.choose_from_list(cache_entry)

        # Update cache
        cache.add(self.name, data)

        return data
