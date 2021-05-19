#!/usr/bin/env python3

import argparse
import glob
import importlib.util
import inspect
import os
import sys

from multiprocessing import Pool

import lib.db
import lib.generator
import lib.random

from lib.base_object import BaseObject


def run(seed, dsn, num_rows, generators):
    print(f'Random generator seeded to: { seed }')
    rand_gen = lib.random.Random(seed=seed)

    for table, gen_cls in generators.items():
        data = gen_cls.sample(rand_gen, num_rows)
        with lib.db.DB(dsn) as dbconn:
            dbconn.ingest_table(table, data)


if __name__ == '__main__':
    args_to_parse = argparse.ArgumentParser()
    args_to_parse.add_argument('--dsn', required=True)
    args_to_parse.add_argument('--rows-per-worker', type=int, required=True)
    args_to_parse.add_argument('--max-processes', type=int, required=True, default=4)
    args_to_parse.add_argument('--workers', type=int, required=True, default=4)
    args = args_to_parse.parse_args()

    generators = {}
    all_modules = glob.glob('generators/*.py')
    for module_path in all_modules:
        module_file = os.path.basename(module_path)
        module_name = os.path.splitext(module_file)[0]

        spec = importlib.util.spec_from_file_location(module_name, module_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module.__name__] = module
        spec.loader.exec_module(module)

        for name, cls in inspect.getmembers(module, inspect.isclass):
            if all((
                cls is not BaseObject,
                issubclass(cls, BaseObject),
                hasattr(cls, 'TABLE_NAME'))):
                generators[cls.TABLE_NAME] = cls

    with lib.db.DB(args.dsn) as db:
        for table_name in generators:
            db.truncate_table(table_name)

    if args.workers == 1:
        run(1, args.dsn, args.rows_per_worker, generators)

    else:
        pargs = [(chunk_idx + 1, args.dsn, args.rows_per_worker, generators) \
                 for chunk_idx in range(args.workers)]

        with Pool(processes=args.max_processes) as pool:
            pool.starmap(run, pargs)
