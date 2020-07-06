"""
This module implements a registry of Python formatters for various metadata
types, along with default implementations.
"""
# python 3.7+ from __future__ import annotations
from typing import Dict, Any, Callable, Optional
from collections import defaultdict
import logging

log = logging.getLogger("arkimet.formatter")


class Formatter:
    """
    Registry for metadata formatters implemented in Python.

    This is used to register formatter implementations, and access them while formatting.

    Use :class:`arkimet.Formatter` to do the actual formatting: it will call
    either into this or in C++ implementations as needed.
    """
    formatters = defaultdict(list)

    def format(self, t: Dict[str, Any]) -> str:
        """
        Look up and call a formatter for the given metadata item.
        """
        # Find the formatter list for this type
        formatters = self.formatters.get(t["type"])
        if formatters is None:
            return None

        # Try all formatters in the list, returning the result of the first
        # that returns not-None.
        # Iterate in reverse order, so that formatters loaded later (like from
        # /etc) can be called earlier and fall back on the shipped ones
        for formatter in reversed(formatters):
            try:
                res = formatter(t)
            except Exception:
                log.exception("formatter failed")
                res = None

            if res is not None:
                return res

        # Otherwise return None
        return None

    @classmethod
    def register(cls, type: str, formatter: Callable[[Dict[str, Any]], Optional[str]]):
        """
        Register a callable as a formatter for the given metadata type.

        A registered callable can return None to fall back on previously
        registered callables. This allows to add formatters for specific cases
        only, without losing the default handling.
        """
        if formatter not in cls.formatters[type]:
            cls.formatters[type].append(formatter)
