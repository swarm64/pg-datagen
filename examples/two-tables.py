
from lib.schema_parser import Schema
from lib.base_object import Dependency


TABLES = {
    'a': Schema('examples/a.sql').parse_create_table(),
    'b': Schema('examples/b.sql').parse_create_table()
}

GRAPH = {
    'a': [
        Dependency(name='b', scaler=0.1)
    ],
    'b': []
}

ENTRYPOINT = Dependency('a', 1)
