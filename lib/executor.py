
import math
import sys

from concurrent.futures import ProcessPoolExecutor, as_completed
from importlib.machinery import SourceFileLoader
from typing import List, Tuple

from lib.base_object import BaseObject, Dependency
from lib.schema_parser import Schema
from lib.db import DB
from lib.random import Random

from loguru import logger


class Executor:
    def __init__(self, args: object) -> None:
        self.args = args

        target = SourceFileLoader('target', args.target).load_module()
        self.graph = target.GRAPH
        self.entrypoint = target.ENTRYPOINT
        self.schemas = {
            table_name: Schema(schema_path).parse_create_table() for
            table_name, schema_path in target.TABLES.items()
        }

        self.none_probabilities = {}
        if hasattr(target, 'NONE_PROBABILITIES'):
            self.none_probabilities = target.NONE_PROBABILITIES

    def _generate_sequence(self):
        assert isinstance(self.entrypoint, Dependency)

        sequence = [self.entrypoint]
        queue = [self.entrypoint]

        while queue:
            item = queue.pop(0)
            assert isinstance(item, Dependency)

            for next_item in self.graph[item.name]:
                if next_item.name not in sequence:
                    sequence.append(next_item)
                    queue.append(next_item)

        return sequence

    def _truncate_tables(self) -> None:
        logger.info('Truncating tables.')
        with DB(self.args.dsn) as db:
            for table_name in self.schemas:
                db.truncate_table(table_name)

    def _get_batches(self) -> List[Tuple[int, int]]:
        total_rows = self.args.total_rows
        batch_size = self.args.batch_size
        batch_sizes = [min(x + batch_size, total_rows) - x
                       for x in range(0, total_rows, batch_size)]

        return [(idx + 1, batch_size) for idx, batch_size in enumerate(batch_sizes)]

    def _run_helper(self, sequence: list, keep_data: dict, seed: int, num_rows: int) -> bool:
        logger.info(f'Generating {num_rows} rows with seed {seed}.')

        data_store = { }
        with DB(self.args.dsn) as dbconn:
            rand_gen = Random(seed=seed)
            for dependency in sequence:
                table_name = dependency.name
                schema = self.schemas[table_name]
                rows_to_gen = max(1, math.ceil(num_rows * dependency.scaler))
                data = BaseObject.sample_from_source(rand_gen, rows_to_gen, schema, data_store)
                dbconn.ingest_table(table_name, schema, data)

                columns = keep_data.get(table_name, [])
                if columns:
                    for row in data:
                        for column in columns:
                            data_path = f'{ table_name }.{ column }'
                            if data_path not in data_store:
                                data_store[data_path] = []

                            data_store[data_path].append(row.get(column))

    def _determine_data_to_keep(self, sequence: list) -> dict:
        keep_data = {}
        for item in sequence:
            for column_name, column_gen in self.schemas[item.name].items():
                if column_gen.gen.startswith('choose_from_list'):
                    table, _, column = column_gen.gen.split(' ')[1].rpartition('.')
                    if table not in keep_data:
                        keep_data[table] = []

                    keep_data[table].append(column)

        return keep_data


    def run(self):
        batches = self._get_batches()
        sequence = self._generate_sequence()
        keep_data = self._determine_data_to_keep(sequence)

        if self.args.truncate:
            self._truncate_tables()

        for batch_id, batch_size in batches:
            self._run_helper(sequence, keep_data, batch_id, batch_size)
        # with ProcessPoolExecutor(self.args.max_parallel_workers) as executor:
        #     all_futures = []
        #     for batch_id, batch_size in batches:
        #         future = executor.submit(
        #             self._run_helper, sequence, keep_data, batch_id, batch_size)
        #         all_futures.append(future)

        #     for future in as_completed(all_futures):
        #         try:
        #             future.result()
        #         except Exception as exc:
        #             logger.exception(exc)
        #             sys.exit(1)
