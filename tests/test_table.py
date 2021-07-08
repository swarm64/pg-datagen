
from collections import OrderedDict

import pytest

from lib.schema_parser import Column
from lib.table import Table


def test_get_column_dependecies(mocker):
    schema_mock = mocker.patch('lib.schema_parser.Schema')
    schema_mock.return_value.parse_create_table.return_value = OrderedDict([
        (1, Column('bla', True, [], 0.5)),
        (2, Column('choose_from_list a.b.c', True, [], 0.5)),
        (3, Column('bleh', True, [], 0.5))
    ])

    table = Table(schema_path='foobar.sql', scaler=0.42)
    deps = table.get_column_dependencies()
    assert deps == set((('a.b', 'c'),))
