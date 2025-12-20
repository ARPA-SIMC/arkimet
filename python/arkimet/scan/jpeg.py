from typing import Callable

# import functools
import io
import logging
from pathlib import Path

from PIL import Image
import arkimet
import arkimet.scan

from .utils.jpeg import ScannedImage

log = logging.getLogger("arkimet.scan.jpeg")


class JPEGScanner(arkimet.scan.DataFormatScannner):
    def __init__(self):
        self.scanners = []
        self.scanners_sorted = True

    def scan_image(self, im: Image, md: arkimet.Metadata):
        self.scan_tags(ScannedImage(im), md)

    def scan_file(self, pathname: Path, md: arkimet.Metadata):
        with Image.open(pathname) as im:
            self.scan_image(im, md)

    def scan_data(self, data: bytes, md: arkimet.Metadata):
        with io.ByteIO(data) as fd:
            with Image.open(fd) as im:
                self.scan_image(im, md)

    def scan_tags(self, sample: ScannedImage, md: arkimet.Metadata):
        if not self.scanners_sorted:
            self.scanners.sort(key=lambda p: p[0])
            self.scanners_sorted = True

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

    def register(self, scanner: Callable[[ScannedImage, arkimet.Metadata], None], priority=0):
        if scanner not in self.scanners:
            self.scanners.append((priority, scanner))
            self.scanners_sorted = False


jpeg_scanner = JPEGScanner()

arkimet.scan.registry.register_scanner("jpeg", jpeg_scanner)
