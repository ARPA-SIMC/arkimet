from typing import Callable
from collections import defaultdict
import arkimet
import _arkimet
import logging

Grib = _arkimet.scan.grib.Grib

log = logging.getLogger("arkimet.scan.grib")


class Scanner:
    by_edition = defaultdict(list)

    def __init__(self):
        for type, scanners in self.by_edition.items():
            scanners.sort(key=lambda p: p[0])

    def scan(self, grib: Grib, md: arkimet.Metadata):
        # Find the formatter list for this style
        scanners = self.by_edition.get(grib.edition)
        if scanners is None:
            return None

        # Try all scanner functions in the list, in priority order, stopping at
        # the first one that returns False.
        # Note that False is explicitly required: returning None will not stop
        # the scanner chain.
        for prio, scan in scanners:
            try:
                if scan(grib, md) is False:
                    break
            except Exception:
                log.exception("scanner function failed")

    @classmethod
    def register(cls, edition: int, scanner: Callable[[Grib, arkimet.Metadata], None], priority=0):
        if scanner not in cls.by_edition[edition]:
            cls.by_edition[edition].append((priority, scanner))
