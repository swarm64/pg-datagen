
from lib.base_object import Table


TABLES = {
    'public.a': Table(schema_path='examples/dependencies/a.sql', scaler=1, none_prob=0),
    'public.b': Table(schema_path='examples/dependencies/b.sql', scaler=10, none_prob= 0)
}

GRAPH = {
    'public.a': [
        'public.b'
    ],
    'public.b': []
}

ENTRYPOINT = 'public.a'
