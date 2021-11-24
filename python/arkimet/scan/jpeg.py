from typing import Callable, Dict, Any
import io
import logging

import arkimet
import exifread

log = logging.getLogger("arkimet.scan.jpeg")


class Scanner:
    scanners = []

    def __init__(self):
        self.scanners.sort(key=lambda p: p[0])

    def scan_file(self, pathname: str, md: arkimet.Metadata):
        with open(pathname, "rb") as fd:
            tags = exifread.process_file(fd, details=False)
        self.scan_tags(tags, md)

    def scan_data(self, data: bytes, md: arkimet.Metadata):
        with io.ByteIO(data) as fd:
            tags = exifread.process_file(fd, details=False)
        self.scan_tags(tags, md)

    def scan_tags(self, tags: Dict[str, Any], md: arkimet.Metadata):
        # Try all scanner functions in the list, in priority order, stopping at
        # the first one that returns False.
        # Note that False is explicitly required: returning None will not stop
        # the scanner chain.
        for prio, scan in self.scanners:
            try:
                if scan(tags, md) is False:
                    break
            except Exception:
                log.warning("scanner function failed", exc_info=True)

    @classmethod
    def register(cls, scanner: Callable[[Dict[str, Any], arkimet.Metadata], None], priority=0):
        if scanner not in cls.scanners:
            cls.scanners.append((priority, scanner))
