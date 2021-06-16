
from lib.base_object import Dependency


TABLES = {
    'public.a': 'examples/dependencies/a.sql',
    'public.b': 'examples/dependencies/b.sql'
}

GRAPH = {
    'public.a': [
        Dependency(name='public.b', scaler=0.1)
    ],
    'public.b': []
}

ENTRYPOINT = Dependency('public.a', 1)
