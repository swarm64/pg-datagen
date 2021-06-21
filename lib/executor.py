
import math
import sys

from concurrent.futures import ProcessPoolExecutor, as_completed
from importlib.machinery import SourceFileLoader
from typing import List, Tuple

from lib.base_object import BaseObject
from lib.db import DB
from lib.random import Random

from loguru import logger


class Executor:
    def __init__(self, args: object) -> None:
        self.args = args

        target = SourceFileLoader('target', args.target).load_module()
        self.graph = target.GRAPH
        self.entrypoint = target.ENTRYPOINT
        self.tables = target.TABLES

    def _generate_sequence(self):
        sequence = [self.entrypoint]
        queue = [self.entrypoint]

        while queue:
            item = queue.pop(0)
            for next_item in self.graph[item]:
                if next_item not in sequence:
                    sequence.append(next_item)
                    queue.append(next_item)

        return sequence

    def _truncate_table(self, table_name: str) -> None:
        logger.info('Truncating tables.')
        with DB(self.args.dsn) as db:
            db.truncate_table(table_name)

    def _vacuum_analyze(self, table) -> None:
        with DB(self.args.dsn) as db:
            db.vacuum_analyze_table(table)

    def _get_batches(self) -> List[Tuple[int, int]]:
        total_rows = self.args.rows
        batch_size = self.args.batch_size
        batch_sizes = [min(x + batch_size, total_rows) - x
                       for x in range(0, total_rows, batch_size)]

        return [(idx + 1, batch_size) for idx, batch_size in enumerate(batch_sizes)]

    @classmethod
    def _update_data_store(cls, table_name: str, data_store: dict, data: list, columns: list):
        for row in data:
            for column in columns:
                data_path = f'{ table_name }.{ column }'
                if data_path not in data_store:
                    data_store[data_path] = []

                data_store[data_path].append(row.get(column))

    @classmethod
    def _get_num_rows_to_gen(cls, rand_gen, num_rows, scaler) -> int:
        if isinstance(scaler, tuple):
            attr = scaler[0]
            args = scaler[1:]
            rows_to_gen = num_rows * getattr(rand_gen, attr)(*args)

        elif callable(scaler):
            rows_to_gen = scaler(rand_gen, num_rows)

        elif isinstance(scaler, (int, float)):
            rows_to_gen = num_rows * scaler

        else:
            raise AttributeError(f'Unknown scaler type: { type(scaler) }')

        return max(1, math.ceil(rows_to_gen))

    def _run_helper(self, sequence: list, keep_data: dict, seed: int, num_rows: int) -> bool:
        data_store = { }
        with DB(self.args.dsn) as dbconn:
            rand_gen = Random(seed=seed)
            for table_name in sequence:
                table = self.tables[table_name]

                rows_to_gen = Executor._get_num_rows_to_gen(
                    rand_gen, num_rows, table.scaler)

                logger.info(f'Generating {rows_to_gen} rows (seed {seed}) for table { table_name }')

                data = BaseObject.sample_from_source(rand_gen, rows_to_gen, table.schema, data_store)
                dbconn.ingest_table(table_name, table.schema, data)

                columns = keep_data.get(table_name, [])
                if columns:
                    Executor._update_data_store(table_name, data_store, data, columns)

    def _determine_data_to_keep(self, sequence: list) -> dict:
        keep_data = {}
        for item in sequence:
            for column_gen in self.tables[item].schema.values():
                if column_gen.gen.startswith('choose_from_list'):
                    table, _, column = column_gen.gen.split(' ')[1].rpartition('.')
                    if table not in keep_data:
                        keep_data[table] = []

                    keep_data[table].append(column)

        return keep_data

    @classmethod
    def _execute_in_parallel(cls, executor, tasks):
        all_futures = []
        for task, args in tasks:
            all_futures.append(executor.submit(task, *args))

        for future in as_completed(all_futures):
            try:
                future.result()
            except Exception as exc:
                logger.exception(exc)
                sys.exit(1)

    def run(self):
        batches = self._get_batches()
        sequence = self._generate_sequence()
        keep_data = self._determine_data_to_keep(sequence)

        with ProcessPoolExecutor(self.args.max_parallel_workers) as executor:
            if self.args.truncate:
                tasks = [(self._truncate_table, (table,)) for table in sequence]
                Executor._execute_in_parallel(executor, tasks)

            tasks = []
            for batch_id, batch_size in batches:
                task = (self._run_helper, (sequence, keep_data, batch_id, batch_size))
                tasks.append(task)
            Executor._execute_in_parallel(executor, tasks)

            if self.args.vacuum_analyze:
                tasks = [(self._vacuum_analyze, (table,)) for table in sequence]
                Executor._execute_in_parallel(executor, tasks)
