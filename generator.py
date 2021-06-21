#!/usr/bin/env python3

import argparse

from lib.executor import Executor


if __name__ == '__main__':
    args_to_parse = argparse.ArgumentParser()
    args_to_parse.add_argument('--dsn', required=True, help=(
        'The DSN to use for ingestion.'))
    args_to_parse.add_argument('--batch-size', type=int, required=True, help=(
        'How many rows a single worker generates'))
    args_to_parse.add_argument('--max-parallel-workers', type=int, default=4, help=(
        'How many parallel processes to use at max.'))
    args_to_parse.add_argument('--rows', type=int, required=True, help=(
        'How many rows to generate for each scaler == 1.'))
    args_to_parse.add_argument('--truncate', action='store_true', default=False, help=(
        'Whether to truncate tables before data generation.'))
    args_to_parse.add_argument('--dry-run', action='store_true', default=False, help=(
        'Whether to do a dry run or not'))
    args_to_parse.add_argument('--vacuum-analyze', action='store_true', default=False, help=(
        'Run a VACUUM-ANALYZE after ingestion.'))
    args_to_parse.add_argument('--target', required=True, help=(
        'The Python file containing defintions for random data generation'))
    args = args_to_parse.parse_args()

    Executor(args).run()
