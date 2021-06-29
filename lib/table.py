
from collections import namedtuple, OrderedDict
from dataclasses import dataclass, field
from typing import Set, Tuple

import lib.schema_parser as schema_parser


Column = namedtuple('Column', ['name', 'rng', 'type'])


@dataclass
class Table:
    schema_path: str
    scaler: float
    schema: OrderedDict = field(init=False)

    def __post_init__(self):
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
