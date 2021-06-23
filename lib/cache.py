"""
This module is responsible for caching data of dependencies.
"""

from typing import AbstractSet, Any, Dict, Mapping, List, Sequence, Tuple

from loguru import logger


class Cache:
    """Cache to store objects required as dependencies later."""

    _cache_map: Dict[str, List[str]]
    _store: Dict[str, List]

    def __init__(self, cache_map_source: AbstractSet[Tuple[str, str]]):
        self._cache_map = Cache._build_cache_map(cache_map_source)

        self._store = {}
        self._prepare_cache_store()

    @classmethod
    def build_path(cls, table: str, column: str) -> str:
        """Build a path for cache-lookups."""
        return f'{ table }.{ column }'

    @classmethod
    def _build_cache_map(cls, source: AbstractSet[Tuple[str, str]]) -> Dict[str, List[str]]:
        logger.debug('Building cache map')
        cache_map = {}
        for table, column in source:
            logger.debug(f'Adding { table }.{ column } to cache map')
            if table not in cache_map:
                cache_map[table] = []
            cache_map[table].append(column)

        return cache_map

    def _prepare_cache_store(self) -> None:
        for table, columns in self._cache_map.items():
            for column in columns:
                path = Cache.build_path(table, column)
                self._store[path] = []

    def add(self, table_name: str, data: Sequence[Mapping[str, Sequence]]) -> None:
        """Cache all columns that need to be cached."""
        columns = self._cache_map.get(table_name)
        if not columns:
            return

        logger.debug(f'Caching { table_name } data for columns { columns }.')
        for row in data:
            for column in columns:
                path = Cache.build_path(table_name, column)
                self._store[path].append(row.get(column))

    def retrieve(self, path: str) -> Sequence[Any]:
        """Retrieve a cached object by its path."""
        return self._store.get(path, [])
