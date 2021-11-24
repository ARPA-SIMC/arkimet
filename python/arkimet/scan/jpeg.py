from typing import Callable
import arkimet
import logging

log = logging.getLogger("arkimet.scan.jpeg")


class Scanner:
    scanners = []

    def __init__(self):
        self.scanners.sort(key=lambda p: p[0])

    def scan_file(self, pathname: str, md: arkimet.Metadata):
        raise NotImplementedError("scan_file")
        # with netCDF4.Dataset(pathname, "r") as f:
        #     # Try all scanner functions in the list, in priority order, stopping at
        #     # the first one that returns False.
        #     # Note that False is explicitly required: returning None will not stop
        #     # the scanner chain.
        #     for prio, scan in self.scanners:
        #         try:
        #             if scan(f, md) is False:
        #                 break
        #         except Exception:
        #             log.warning("scanner function failed", exc_info=True)

    def scan_data(self, data: bytes, md: arkimet.Metadata):
        raise NotImplementedError("scan_data")

    @classmethod
    def register(cls, scanner: Callable[["netCDF4.Dataset", arkimet.Metadata], None], priority=0):
        if scanner not in cls.scanners:
            cls.scanners.append((priority, scanner))
