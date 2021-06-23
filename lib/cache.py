"""
This module is responsible for caching data of dependencies.
"""

from typing import Any, Mapping, Sequence

from loguru import logger


class Cache:
    """Cache to store objects required as dependencies later."""
    def __init__(self, tables: Mapping[str, object]):
        self._store = {}

        self._cache_map = {}
        self._determine_data_to_keep(tables)

    def _determine_data_to_keep(self, tables: Mapping[str, object]) -> None:
        """Parse all tables locating what needs to be cached."""
        logger.debug('Determining what to cache')
        for table in tables.values():
            for column_gen in table.schema.values():
                if column_gen.gen.startswith('choose_from_list'):
                    path = column_gen.gen.split(' ')[1]
                    table, _, column = path.rpartition('.')
                    self._add_to_cache_map(table, column)

    def _add_to_cache_map(self, table: str, column: str) -> None:
        logger.debug(f'Adding { table }.{ column } to cache map')
        if table not in self._cache_map:
            self._cache_map[table] = []
        self._cache_map[table].append(column)

    def _add_to_data_store(self, path: str, datum: Any) -> None:
        if path not in self._store:
            self._store[path] = []
        self._store[path].append(datum)

    def flush(self) -> None:
        """Flush the cache without removing information what to cache."""
        self._store.clear()

    def add_to_cache(self, table_name: str, data: Sequence) -> None:
        """Cache all columns that need to be cached."""
        columns = self._cache_map.get(table_name)
        if not columns:
            return

        logger.debug(f'Caching { table_name } data for columns { columns }.')
        for row in data:
            for column in columns:
                path = f'{ table_name }.{ column }'
                self._add_to_data_store(path, row.get(column))

    def get_from_cache(self, path: str) -> Sequence[Any]:
        """Retrieve a cached object by its path."""
        return self._store[path]
