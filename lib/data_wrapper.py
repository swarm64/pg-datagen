"""
Base class supplying core methods for sampled random objects.
"""

from collections import OrderedDict
from typing import Any, List, Type

# from mimesis.schema import Schema

# from lib.random import Random


class DataWrapper:
    """Class to mainly inherit from and wrap raw data into."""

    def __init__(self, raw: Type[OrderedDict]):
        if not isinstance(raw, OrderedDict):
            raise TypeError('raw must be a OrderedDict')

        self.raw = raw

    def __getitem__(self, key):
        return self.raw[key]

    def __setitem__(self, key, value):
        self.raw[key] = value

    def __eq__(self, other):
        return self.raw == other.raw

    def to_sql(self) -> str:
        """Convert this object to an SQL string.
        """
        def to_str(value):
            if value is None:
                return ''

            return str(value)

        return '|'.join([to_str(value) for value in self.raw.values()])

    def set(self, key: str, value: Any):
        """Set the value of an entry in the underlying map."""
        self.raw[key] = value

    def get(self, key: str) -> Any:
        """Retrieve a value from the underlying map."""
        return self.raw.get(key)
