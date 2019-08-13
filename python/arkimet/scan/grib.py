from typing import Callable
from collections import defaultdict
import arkimet
import _arkimet
import logging

Grib = _arkimet.scan.grib.Grib

log = logging.getLogger("arkimet.scan.grib")


class Scanner:
    by_edition = defaultdict(list)

    def scan(self, grib: Grib, md: arkimet.Metadata):
        # Find the formatter list for this style
        scanners = self.by_edition.get(grib.edition)
        if scanners is None:
            return None

        # Try all scanner functions in the list, returning the result of the
        # first that returns not-None.
        # Iterate in reverse order, so that scanner functions loaded later
        # (like from /etc) can be called earlier and fall back on the shipped
        # ones
        for scan in reversed(scanners):
            try:
                scan(grib, md)
            except Exception:
                log.exception("scanner function failed")

    @classmethod
    def register(cls, edition: int, scanner: Callable[[Grib, arkimet.Metadata], None]):
        if scanner not in cls.by_edition[edition]:
            cls.by_edition[edition].append(scanner)
