
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

    table = Table('foo.bar', schema_path='foobar.sql', scaler=0.42)
    deps = table.get_column_dependencies()
    assert deps == set((('a.b.c', 'foo.bar.2'),))


# def test_generate_data(mocker):
#     schema_mock = mocker.patch('mimesis.schema.Schema.create')
#     schema_mock.return_value = [
#         OrderedDict([(1, 2)]),
#         OrderedDict([(3, 4)]),
#         OrderedDict([(5, 6)])
#     ]
#
#     objects = DataWrapper.run_sampling(lambda: None, 3)
#     assert objects == [
#         DataWrapper(OrderedDict([(1, 2)])),
#         DataWrapper(OrderedDict([(3, 4)])),
#         DataWrapper(OrderedDict([(5, 6)]))
#     ]
#
#     schema_mock.assert_called_once_with(iterations=3)
#
#
# def test_schema_from_source(mocker):
#     rand_gen_mock = mocker.MagicMock()
#     cache_mock = mocker.MagicMock()
#
#     gen_column_mock = mocker.patch('lib.base_object.DataWrapper._generate_column')
#     gen_column_mock.return_value = 42
#
#     schema = DataWrapper.schema_from_source(rand_gen_mock, {
#         'foo': Column('xyz', True, [1, 2, 3], None),
#         'bleh': Column('skip', False, [1, 2, 3], None),
#         'bar': Column('bla', True, [1, 2, 3], 0.1),
#     }, cache_mock)
#
#     assert schema() == OrderedDict([('foo', 42), ('bar', 42)])
#
#
# def test_run_sampling(mocker):
#     schema_mock = mocker.patch('mimesis.schema.Schema.create')
#     schema_mock.return_value = [
#         OrderedDict([(1, 2)]),
#         OrderedDict([(3, 4)]),
#         OrderedDict([(5, 6)])
#     ]
#
#     objects = DataWrapper.run_sampling(lambda: None, 3)
#     assert objects == [
#         DataWrapper(OrderedDict([(1, 2)])),
#         DataWrapper(OrderedDict([(3, 4)])),
#         DataWrapper(OrderedDict([(5, 6)]))
#     ]
#
#     schema_mock.assert_called_once_with(iterations=3)
