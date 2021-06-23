"""
This module controls execution of the random data generator.
"""

import math
import sys

from concurrent.futures import ProcessPoolExecutor, as_completed
from importlib.machinery import SourceFileLoader
from typing import Any, Callable, Dict, Mapping, List, Sequence, Tuple, Type, Union

from lib.base_object import BaseObject
from lib.cache import Cache
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

    def _generate_sequence(self) -> List[str]:
        """Traverse the graph in BFS manner creating a linear execution order."""
        sequence = [self.entrypoint]
        queue = [self.entrypoint]

        while queue:
            item = queue.pop(0)
            for next_item in self.graph[item]:
                if next_item not in sequence:
                    sequence.append(next_item)
                    queue.append(next_item)

        return sequence

    def truncate_table(self, table_name: str) -> None:
        """Helper function to truncate a table by name."""
        logger.info('Truncating tables.')
        with DB(self.args.dsn) as db:
            db.truncate_table(table_name)

    def vacuum_analyze(self, table) -> None:
        """Helper function to run VACUUM-ANALYZE on a table."""
        with DB(self.args.dsn) as db:
            db.vacuum_analyze_table(table)

    def _get_batches(self) -> List[Tuple[int, int]]:
        """Calculate batches based on runtime arguments."""
        total_rows = self.args.rows
        batch_size = self.args.batch_size
        batch_sizes = [min(x + batch_size, total_rows) - x
                       for x in range(0, total_rows, batch_size)]

        return [(idx + 1, batch_size) for idx, batch_size in enumerate(batch_sizes)]

    @classmethod
    def _get_num_rows_to_gen(cls, rand_gen: Type[Random], num_rows: int,
                             scaler: Union[Tuple, int, float,
                                     Callable[[Type[Random], int], None]]) -> int:
        """Helper function to determine how many rows shall be generated."""
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

    def _run_helper(self, sequence: Sequence[str], cache: Type[Cache],
                    seed: int, num_rows: int) -> None:
        with DB(self.args.dsn) as dbconn:
            rand_gen = Random(seed=seed)
            for table_name in sequence:
                table = self.tables[table_name]

                rows_to_gen = Executor._get_num_rows_to_gen(
                    rand_gen, num_rows, table.scaler)

                logger.info(f'Generating {rows_to_gen} rows (seed {seed}) for table { table_name }')

                data = BaseObject.sample_from_source(rand_gen, rows_to_gen, table.schema, cache)
                dbconn.ingest_table(table_name, table.schema, data)
                cache.add_to_cache(table_name, data)

    @classmethod
    def _execute_in_parallel(cls, executor: Type[ProcessPoolExecutor],
                             tasks: Tuple[Callable[[Any], None], Tuple[Any, ...]]):
        """Run set of tasks in parallel using the provided executor."""
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
        """Main entrypoint to start the random data generator."""
        batches = self._get_batches()
        sequence = self._generate_sequence()
        cache = Cache(self.tables)

        with ProcessPoolExecutor(self.args.max_parallel_workers) as executor:
            if self.args.truncate:
                tasks = [(self.truncate_table, (table,)) for table in sequence]
                Executor._execute_in_parallel(executor, tasks)

            tasks = []
            for batch_id, batch_size in batches:
                task = (self._run_helper, (sequence, cache, batch_id, batch_size))
                tasks.append(task)
            Executor._execute_in_parallel(executor, tasks)

            if self.args.vacuum_analyze:
                tasks = [(self.vacuum_analyze, (table,)) for table in sequence]
                Executor._execute_in_parallel(executor, tasks)
