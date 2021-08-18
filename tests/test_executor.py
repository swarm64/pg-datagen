
import pytest

from lib.executor import Executor
from lib.table import Table


@pytest.fixture
def executor(mocker):
    mocker.patch('lib.schema_parser.Schema')

    class ExecutorFixture(Executor):
        def __init__(self):
            self.tables = [
                Table('a.a', schema_path='bla/a.sql', scaler=1),
                Table('a.b', schema_path='bleh/b.sql', scaler=10),
                Table('a.c', schema_path='bleh/c.sql', scaler=0.1)
            ]

    return ExecutorFixture()


def test_generate_sequence(executor):
    executor.graph = {
        'a.a': ['a.c', 'a.b'],
        'a.b': [],
        'a.c': ['a.b'],
    }
    executor.enrypoint = ['a.a']

    sequence = [table.name for table in executor._generate_sequence()]
    assert sequence == ['a.a', 'a.c', 'a.b']


def test_sequence_no_deps(executor):
    executor.graph = {
        'a.a': [],
        'a.b': [],
        'a.c': []
    }
    executor.enrypoint = ['a.a', 'a.b', 'a.c']

    sequence = [table.name for table in executor._generate_sequence()]
    assert sequence == ['a.c', 'a.b', 'a.a']
