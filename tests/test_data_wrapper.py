
from collections import OrderedDict

import pytest

from lib.data_wrapper import DataWrapper
from lib.schema_parser import Column


def test_init():
    with pytest.raises(TypeError):
        DataWrapper({})

    raw = OrderedDict([('3', 1), ('2', 2), ('1', 3)])
    obj = DataWrapper(raw)
    assert obj


def test_getters():
    raw = OrderedDict([('3', 1), ('2', 2), ('1', 3)])
    obj = DataWrapper(raw)

    assert obj['2'] == 2
    assert obj['1'] == 3
    assert obj['3'] == 1

    assert obj.get('1') == 3
    assert obj.get('2') == 2
    assert obj.get('3') == 1


def test_setters():
    raw = OrderedDict([('3', 1), ('2', 2), ('1', 3)])
    obj = DataWrapper(raw)

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


def test_eq():
    a = OrderedDict([(1, 2), (3, 4)])
    b = OrderedDict([(1, 2), (3, 4)])
    c = OrderedDict([(1, 2), (3, 5)])

    assert DataWrapper(a) == DataWrapper(a)
    assert DataWrapper(a) == DataWrapper(b)
    assert DataWrapper(a) != DataWrapper(c)


def test_to_sql():
    raw = OrderedDict([('a', 2), ('b', None), ('c', 4)])
    sql = DataWrapper(raw).to_sql()
    assert sql == '2||4'
