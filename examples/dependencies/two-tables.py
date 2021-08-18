
from lib.table import Table


TABLES = [
    Table('public.a', schema_path='examples/dependencies/a.sql', scaler=1),
    Table('public.b', schema_path='examples/dependencies/b.sql', scaler=10)
]

