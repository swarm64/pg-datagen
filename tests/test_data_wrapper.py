
from collections import OrderedDict

import pytest

from lib.data_wrapper import DataWrapper
from lib.schema_parser import Column


def test_init():
    with pytest.raises(TypeError):
        BaseObject({})

    raw = OrderedDict([('3', 1), ('2', 2), ('1', 3)])
    obj = BaseObject(raw)
    assert obj


def test_getters():
    raw = OrderedDict([('3', 1), ('2', 2), ('1', 3)])
    obj = BaseObject(raw)

    assert obj['2'] == 2
    assert obj['1'] == 3
    assert obj['3'] == 1

    assert obj.get('1') == 3
    assert obj.get('2') == 2
    assert obj.get('3') == 1


def test_setters():
    raw = OrderedDict([('3', 1), ('2', 2), ('1', 3)])
    obj = BaseObject(raw)

    with pytest.raises(KeyError):
        x = obj['4']

    obj['4'] = 42
    assert obj['4'] == 42

    with pytest.raises(KeyError):
        x = obj['5']

    obj.set('5', 43)
    assert obj['5'] == 43

    assert obj['1'] == 3
    obj['1'] = 'bla'
    assert obj['1'] == 'bla'


def test_generate_columns(mocker):
    rand_gen_mock = mocker.MagicMock()
    cache_mock = mocker.MagicMock()
    column_gen = Column('foo', True, [1, 2, 3], None)
    BaseObject._generate_column(rand_gen_mock, column_gen, cache_mock)

    rand_gen_mock.bool_sample.assert_not_called()
    rand_gen_mock.foo.assert_called_once_with(1, 2, 3)
    cache_mock.retrieve.assert_not_called()


def test_generate_columns_none_prob_gen(mocker):
    rand_gen_mock = mocker.MagicMock()
    rand_gen_mock.bool_sample.return_value = False

    cache_mock = mocker.MagicMock()
    column_gen = Column('foo', True, [1, 2, 3], 0.9)
    BaseObject._generate_column(rand_gen_mock, column_gen, cache_mock)

    rand_gen_mock.bool_sample.assert_called_once_with(0.9)
    rand_gen_mock.foo.assert_called_once_with(1, 2, 3)
    cache_mock.retrieve.assert_not_called()


def test_generate_columns_none_prob_nogen(mocker):
    rand_gen_mock = mocker.MagicMock()
    rand_gen_mock.bool_sample.return_value = True

    cache_mock = mocker.MagicMock()
    column_gen = Column('foo', True, [1, 2, 3], 0.11)
    BaseObject._generate_column(rand_gen_mock, column_gen, cache_mock)

    rand_gen_mock.bool_sample.assert_called_once_with(0.11)
    rand_gen_mock.foo.assert_not_called()
    cache_mock.retrieve.assert_not_called()


def test_generate_columns_from_list(mocker):
    rand_gen_mock = mocker.MagicMock()
    cache_mock = mocker.MagicMock()
    column_gen = Column('choose_from_list a.b.c', True, [1, 2, 3], None)
    BaseObject._generate_column(rand_gen_mock, column_gen, cache_mock)

    cache_mock.retrieve.assert_called_once_with('a.b.c')
    rand_gen_mock.choose_from_list.assert_called_once()
    rand_gen_mock.bool_sample.assert_not_called()
    rand_gen_mock.foo.assert_not_called()


def test_schema_from_source(mocker):
    rand_gen_mock = mocker.MagicMock()
    cache_mock = mocker.MagicMock()

    gen_column_mock = mocker.patch('lib.base_object.BaseObject._generate_column')
    gen_column_mock.return_value = 42

    schema = BaseObject.schema_from_source(rand_gen_mock, {
        'foo': Column('xyz', True, [1, 2, 3], None),
        'bleh': Column('skip', False, [1, 2, 3], None),
        'bar': Column('bla', True, [1, 2, 3], 0.1),
    }, cache_mock)

    assert schema() == OrderedDict([('foo', 42), ('bar', 42)])


def test_run_sampling(mocker):
    schema_mock = mocker.patch('mimesis.schema.Schema.create')
    schema_mock.return_value = [
        OrderedDict([(1, 2)]),
        OrderedDict([(3, 4)]),
        OrderedDict([(5, 6)])
    ]

    objects = BaseObject.run_sampling(lambda: None, 3)
    assert objects == [
        BaseObject(OrderedDict([(1, 2)])),
        BaseObject(OrderedDict([(3, 4)])),
        BaseObject(OrderedDict([(5, 6)]))
    ]

    schema_mock.assert_called_once_with(iterations=3)


def test_eq():
    a = OrderedDict([(1, 2), (3, 4)])
    b = OrderedDict([(1, 2), (3, 4)])
    c = OrderedDict([(1, 2), (3, 5)])

    assert BaseObject(a) == BaseObject(a)
    assert BaseObject(a) == BaseObject(b)
    assert BaseObject(a) != BaseObject(c)


def test_to_sql():
    raw = OrderedDict([('a', 2), ('b', None), ('c', 4)])
    sql = BaseObject(raw).to_sql()
    assert sql == '2||4'
