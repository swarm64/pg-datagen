
import pytest

from lib.executor import Executor
from lib.table import Table


@pytest.fixture
def executor(mocker):
    mocker.patch('lib.schema_parser.Schema')

    class ExecutorFixture(Executor):
        def __init__(self):
            self.tables = {
                'a': Table(schema_path='bla/a.sql', scaler=1),
                'b': Table(schema_path='bleh/b.sql', scaler=10),
                'c': Table(schema_path='bleh/c.sql', scaler=0.1)
            }

            self.graph = {
                'a': ['c', 'b'],
                'b': [],
                'c': ['b'],
            }

            self.entrypoint = 'a'

    return ExecutorFixture()


def test_generate_sequence(executor):
    sequence = executor._generate_sequence()
    assert sequence == ['a', 'c', 'b']

def test_sequence_no_deps(executor):
    executor.graph = { 'x': [], 'y': [], 'z': [] }
    executor.entrypoint = ['x', 'y', 'z']

    sequence = executor._generate_sequence()
    assert sequence == ['x', 'y', 'z']
