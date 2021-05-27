
import glob
import importlib.util
import inspect
import os
import sys

from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Tuple

from lib.base_object import BaseObject
from lib.db import DB
from lib.random import Random

from loguru import logger


class Executor:
    def __init__(self, args: object) -> None:
        self.args = args
        self.generators = Executor._collect_generators()

    @classmethod
    def _collect_generators(cls) -> dict:
        generators = {}
        all_modules = glob.glob('generators/*.py')
        for module_path in all_modules:
            module_file = os.path.basename(module_path)
            module_name = os.path.splitext(module_file)[0]

            spec = importlib.util.spec_from_file_location(module_name, module_path)
            module = importlib.util.module_from_spec(spec)
            sys.modules[module.__name__] = module
            spec.loader.exec_module(module)

            for _, mod_cls in inspect.getmembers(module, inspect.isclass):
                is_generator_class = all((
                    mod_cls is not BaseObject,
                    issubclass(mod_cls, BaseObject),
                    hasattr(mod_cls, 'TABLE_NAME')))

                if is_generator_class:
                    generators[mod_cls.TABLE_NAME] = {
                        'class': mod_cls
                    }

        return generators

    def _truncate_tables(self) -> None:
        logger.info('Truncating tables.')
        with DB(self.args.dsn) as db:
            for table_name in self.generators:
                if table_name:
                    db.truncate_table(table_name)

    def _get_batches(self) -> List[Tuple[int, int]]:
        total_rows = self.args.total_rows
        batch_size = self.args.batch_size
        batch_sizes = [min(x + batch_size, total_rows) - x
                       for x in range(0, total_rows, batch_size)]

        return [(idx + 1, batch_size) for idx, batch_size in enumerate(batch_sizes)]

    def _run_helper(self, seed: int, num_rows: int) -> bool:
        logger.info(f'Generating {num_rows} rows with seed {seed}.')

        with DB(self.args.dsn) as dbconn:
            rand_gen = Random(seed=seed)
            for gen in self.generators.values():
                if gen['class'].BYPASS:
                    continue

                data = gen['class'].sample(rand_gen, num_rows)
                for table_name, data in data.items():
                    dbconn.ingest_table(table_name, data)

    def run(self):
        if self.args.truncate:
            self._truncate_tables()

        batches = self._get_batches()
        with ProcessPoolExecutor(self.args.max_parallel_workers) as executor:
            all_futures = []
            for batch_id, batch_size in batches:
                future = executor.submit(self._run_helper, batch_id, batch_size)
                all_futures.append(future)

            for future in as_completed(all_futures):
                try:
                    future.result()
                except Exception as exc:
                    logger.exception(exc)
                    sys.exit(1)
