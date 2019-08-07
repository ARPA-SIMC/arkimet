from typing import Dict, Any, Callable, Optional
from collections import defaultdict


class Formatter:
    formatters = defaultdict(set)

    def format(self, t: Dict[str, Any]) -> str:
        # Find the formatter list for this type
        formatters = self.formatters.get(t["type"])
        if formatters is None:
            return None

        # Try all formatters in the list, returning the result of the first
        # that returns not-None
        for formatter in formatters:
            res = formatter(t)
            if res is not None:
                return res

        # Otherwise return None
        return None

    @classmethod
    def register(cls, type: str, formatter: Callable[[Dict[str, Any]], Optional[str]]):
        cls.formatters[type].add(formatter)
