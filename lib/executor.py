"""
This module controls execution of the random data generator.
"""

import math
import operator
import sys

from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import reduce
from importlib.machinery import SourceFileLoader
from typing import AbstractSet, Any, Callable, Mapping, List, Sequence, Tuple, Type, Union

from lib.cache import Cache
from lib.db import DB
from lib.random import Random
from lib.table import Table

from loguru import logger


class Executor:
    graph: Mapping[str, Sequence[str]]
    entrypoint: str
    tables: Mapping[str, Type[Table]]

    def __init__(self, args: object) -> None:
        self.args = args

        target = SourceFileLoader('target', args.target).load_module()
        self.entrypoint = []
        self.tables = target.TABLES
        self.graph = { table.name: [] for table in self.tables }

    def _generate_sequence(self) -> List[str]:
        """Apply topological sort to the graph to find an execution sequence."""
        def sort_util(table_name, visited, stack):
            visited[table_name] = True
            for dep in self.graph[table_name]:
                if not visited[dep]:
                    sort_util(dep, visited, stack)

            table = self._get_table_by_name(table_name)
            stack.insert(0, table)

        visited = {table_name: False for table_name in self.graph.keys()}
        stack = []

        for table in self.tables:
            if not visited[table.name]:
                sort_util(table.name, visited, stack)

        return stack

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

    def _run_helper(self, sequence: Sequence[Table],
                    deps: AbstractSet[Tuple[str, str]], seed: int,
                    num_rows: int) -> None:
        cache = Cache(deps)

        with DB(self.args.dsn) as dbconn:
            rand_gen = Random(seed=seed)
            for table in sequence:
                rows_to_gen = Executor._get_num_rows_to_gen(
                    rand_gen, num_rows, table.scaler)

                logger.info(f'Generating {rows_to_gen} rows (seed {seed}) for table { table.name }')

                data = table.generate_data(rand_gen, rows_to_gen, cache)
                dbconn.ingest_table(table.name, table.schema, data)

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

    def _run_db_cmd_on_table(self, cmd: str, table_name: str) -> None:
        with DB(self.args.dsn) as db:
            if cmd == 'truncate':
                db.truncate_table(table_name)

            elif cmd == 'vacuum-analyze':
                db.vacuum_analyze_table(table_name)

            else:
                raise ValueError(f'Unknown DB command: { cmd }')

    def _get_table_by_name(self, table_name: str) -> Table:
        for table in self.tables:
            if table.name == table_name:
                return table

        raise ValueError(f'Could not get table {table_name}')

    def run(self):
        """Main entrypoint to start the random data generator."""
        batches = self._get_batches()

        all_deps = set()
        for table in self.tables:
            deps = table.get_column_dependencies()
            all_deps.update(deps)

        for source, target in all_deps:
            # Ignore self-deps
            if not target:
                continue

            source_table = source.rpartition('.')[0]
            target_table = target.rpartition('.')[0]
            self.graph[source_table].append(target_table)

        table_deps = set(reduce(operator.add, self.graph.values()))
        self.entrypoint = [table.name for table in self.tables
                           if table.name not in table_deps]

        assert self.entrypoint, 'Entrypoint(s) must not be empty.'
        sequence = self._generate_sequence()

        logger.debug(f'Determined sequence: { sequence }')

        with ProcessPoolExecutor(self.args.max_parallel_workers) as executor:
            if self.args.truncate:
                tasks = [(self._run_db_cmd_on_table, ('truncate', table.name)) for table in sequence]
                Executor._execute_in_parallel(executor, tasks)

            tasks = []
            for batch_id, batch_size in batches:
                task = (self._run_helper, (sequence, all_deps, batch_id, batch_size))
                tasks.append(task)
            Executor._execute_in_parallel(executor, tasks)

            if self.args.vacuum_analyze:
                tasks = [(self._run_db_cmd_on_table, ('vacuum-analyze', table.name)) for table in sequence]
                Executor._execute_in_parallel(executor, tasks)
