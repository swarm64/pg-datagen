
import io

import psycopg2

from loguru import logger


class DB:
    def __init__(self, dsn):
        self.conn = None
        self.cur = None
        self.dsn = dsn

    def __enter__(self):
        self.conn = psycopg2.connect(self.dsn)
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

        return self

    def __exit__(self, type, value, traceback):
        if self.cur:
            self.cur.close()

        if self.conn:
            self.conn.close()

    @classmethod
    def _objs_to_csv(cls, objs):
        data = io.StringIO()
        for obj in objs:
            data.write(obj.to_sql() + '\n')

        data.seek(0)
        return data

    def ingest_table(self, table: str, schema: dict, objs: list):
        logger.info(f'Ingesting { table }: { len(objs) }')

        columns = ','.join(
            [f'"{ name }"' for name, column in schema.items() if column.gen != 'skip'])

        self.cur.copy_expert(f'''
            COPY { table }({ columns })
            FROM STDIN
            WITH(FORMAT CSV, DELIMITER '|')''', DB._objs_to_csv(objs))

    def truncate_table(self, table):
        logger.info(f'Truncating { table }')
        self.cur.execute(f'TRUNCATE { table } CASCADE')
