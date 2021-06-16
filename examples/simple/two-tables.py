
from lib.base_object import Dependency


TABLES = {
    'a': 'examples/simple/a.sql',
    'b': 'examples/simple/b.sql'
}

GRAPH = {
    'a': [
        Dependency(name='b', scaler=0.1)
    ],
    'b': []
}

ENTRYPOINT = Dependency('a', 1)
