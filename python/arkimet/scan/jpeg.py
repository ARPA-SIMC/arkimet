from typing import Callable

# import functools
import io
import logging

from PIL import Image
import arkimet

from .utils.jpeg import ScannedImage

log = logging.getLogger("arkimet.scan.jpeg")


class Scanner:
    scanners = []

    def __init__(self):
        self.scanners.sort(key=lambda p: p[0])

    def scan_image(self, im: Image, md: arkimet.Metadata):
        self.scan_tags(ScannedImage(im), md)

    def scan_file(self, pathname: str, md: arkimet.Metadata):
        with Image.open(pathname) as im:
            self.scan_image(im, md)

    def scan_data(self, data: bytes, md: arkimet.Metadata):
        with io.ByteIO(data) as fd:
            with Image.open(fd) as im:
                self.scan_image(im, md)

    def scan_tags(self, sample: ScannedImage, md: arkimet.Metadata):
        # Try all scanner functions in the list, in priority order, stopping at
        # the first one that returns False.
        # Note that False is explicitly required: returning None will not stop
        # the scanner chain.
        for prio, scan in self.scanners:
            try:
                if scan(sample, md) is False:
                    break
            except Exception:
                log.warning("scanner function failed", exc_info=True)

    @classmethod
    def register(cls, scanner: Callable[[ScannedImage, arkimet.Metadata], None], priority=0):
        if scanner not in cls.scanners:
            cls.scanners.append((priority, scanner))
