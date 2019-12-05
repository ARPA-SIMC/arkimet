from typing import Dict, Any, Callable, Optional
from collections import defaultdict
import logging

log = logging.getLogger("arkimet.formatter")


class Formatter:
    formatters = defaultdict(list)

    def format(self, t: Dict[str, Any]) -> str:
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
        if formatter not in cls.formatters[type]:
            cls.formatters[type].append(formatter)
