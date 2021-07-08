"""
This module provides core functionality for database access.
"""

from io import StringIO
from typing import Mapping, Sequence, Type

import psycopg2

from loguru import logger

from lib.base_object import BaseObject
from lib.table import Column


class DB:
    """Helper class to provide core database functionality, e.g., running queries."""
    def __init__(self, dsn: str):
        self.conn = None
        self.cur = None
        self.dsn = dsn

    def __enter__(self):
        self.conn = psycopg2.connect(self.dsn)
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

        return self

    def __exit__(self, typ, value, traceback):
        if self.cur:
            self.cur.close()

        if self.conn:
            self.conn.close()

    @classmethod
    def _objs_to_csv(cls, objs: Sequence[Type[BaseObject]]) -> Type[StringIO]:
        data = StringIO()
        for obj in objs:
            data.write(obj.to_sql() + '\n')

        data.seek(0)
        return data

    def ingest_table(self, table: str, schema: Mapping[str, Type[Column]],
                     objs: Sequence[BaseObject]):
        """Ingest provided data into the target table."""
        logger.info(f'Ingesting { table }: { len(objs) }')

        columns = ','.join(
            [f'"{ name }"' for name, column in schema.items() if column.gen != 'skip'])

        self.cur.copy_expert(f'''
            COPY { table }({ columns })
            FROM STDIN
            WITH(FORMAT CSV, DELIMITER '|')''', DB._objs_to_csv(objs))

    def truncate_table(self, table: str):
        """Truncate the target table."""
        logger.info(f'Truncating { table }')
        self.cur.execute(f'TRUNCATE { table } CASCADE')

    def vacuum_analyze_table(self, table: str):
        """VACUUM-ANALYZE the target table."""
        logger.info(f'Running VACUUM-ANALYZE on { table }')
        self.cur.execute(f'VACUUM ANALYZE { table }')
