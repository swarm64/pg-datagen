"""
This module is responsible for caching data of dependencies.
"""

from typing import AbstractSet, Any, Dict, Mapping, List, Sequence, Tuple

from loguru import logger


class Cache:
    """Cache to store objects required as dependencies later."""

    _store: Dict[str, List]

    def __init__(self, cache_map_source: AbstractSet[Tuple[str, str]]):
        self._store = {source_path: [] for source_path, _ in cache_map_source}

    @classmethod
    def build_path(cls, table: str, column: str) -> str:
        """Build a path for cache-lookups."""
        return f'{ table }.{ column }'

    def add(self, table_name: str, data: Sequence[Mapping[str, Sequence]]) -> None:
        """Cache all columns that need to be cached."""
        column_paths = [column_path for column_path in self._store.keys()
                        if column_path.startswith(table_name)]
        if not column_paths:
            return

        logger.debug(f'Caching { table_name } data for paths { column_paths }.')
        for row in data:
            for column_path in column_paths:
                column_name = column_path.rpartition('.')[2]
                self._store[column_path].append(row.get(column_name))

    def retrieve(self, path: str) -> Sequence[Any]:
        """Retrieve a cached object by its path."""
        return self._store.get(path, [])
