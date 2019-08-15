from typing import Callable
import arkimet
import h5py
import logging

log = logging.getLogger("arkimet.scan.odimh5")


class Scanner:
    scanners = []

    def scan(self, pathname: str, md: arkimet.Metadata):
        with h5py.File(pathname, "r") as f:
            # Try all scanner functions in the list, returning the result of the
            # first that returns not-None.
            # Iterate in reverse order, so that scanner functions loaded later
            # (like from /etc) can be called earlier and fall back on the shipped
            # ones
            for scan in reversed(self.scanners):
                try:
                    scan(f, md)
                except Exception:
                    log.exception("scanner function failed")

    @classmethod
    def register(cls, scanner: Callable[[h5py.File, arkimet.Metadata], None]):
        if scanner not in cls.scanners:
            cls.scanners.append(scanner)
