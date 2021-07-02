
import pytest

from lib.db import DB


DSN = 'postgresql://postgres@nohost/nodb'


@pytest.fixture(autouse=True)
def mock_connect(mocker):
    return mocker.patch('psycopg2.connect')


@pytest.fixture
def mock_data_obj(mocker):
    obj_mock = mocker.MagicMock()
    obj_mock.to_sql.return_value = '1,2,3'
    return obj_mock


def test_ctx_manager(mock_connect):
    db = None
    with DB(DSN) as db:
        mock_connect.assert_called_once_with(DSN)
        assert db.conn.autocommit

    assert db
    db.cur.close.assert_called_once()
    db.conn.close.assert_called_once()


def test_ctx_manager_exit_no_obj(mock_connect):
    db = None
    with DB(DSN) as db:
        db.conn = None
        db.cur = None

    assert db


def test_objs_to_csv(mock_data_obj):
    retval = DB._objs_to_csv([mock_data_obj, mock_data_obj])
    assert retval.read() == '1,2,3\n1,2,3\n'


def test_ingest_table(mock_data_obj, mocker):
    column = mocker.MagicMock()
    skip_column = mocker.MagicMock()
    skip_column.gen = 'skip'

    table = 'bla'
    schema = {
        'a': column,
        'b': column,
        'xx': skip_column,
        'c': column
    }

    with DB(DSN) as db:
        db.ingest_table(table, schema, [mock_data_obj, mock_data_obj])

    db.cur.copy_expert.assert_called_once()
    first_call = db.cur.copy_expert.mock_calls[0]

    def cleanup(string):
        return string.replace('\n', '').replace(' ', '')

    ref = "COPY bla(\"a\",\"b\",\"c\") FROM STDIN WITH(FORMAT CSV, DELIMITER '|')"
    assert cleanup(first_call.args[0]) == cleanup(ref)


def test_truncate_table():
    db = None
    with DB(DSN) as db:
        db.truncate_table('foobar')

    db.cur.execute.assert_called_once_with('TRUNCATE foobar CASCADE')


def test_vacuum_analyze_table():
    db = None
    with DB(DSN) as db:
        db.vacuum_analyze_table('foobar')

    db.cur.execute.assert_called_once_with('VACUUM ANALYZE foobar')
