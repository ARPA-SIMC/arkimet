# import functools
import io
import logging
from pathlib import Path
from typing import Callable, List, Tuple, Iterable, IO

from PIL import Image

import arkimet
import arkimet.scan

from .utils.jpeg import ScannedImage

log = logging.getLogger("arkimet.scan.jpeg")


class JPEGScanner(arkimet.scan.DataFormatScanner):
    """
    Scanner for JPEG files.

    This is registered with the arkimet library as the entry point for managing
    JPEG data.

    Scanner configuration scripts for JPEG files need to register with this
    class to be executed.
    """

    def __init__(self) -> None:
        """Initialize the JPEGScanner with an empty plugin list."""
        self.scanners: List[
            Tuple[int, Callable[[ScannedImage, arkimet.Metadata], None]]
        ] = []
        self.scanners_sorted: bool = True

    def scan_image(self, im: Image, md: arkimet.Metadata) -> None:
        """Scan a JPEG image as a PIL Image instance."""
        self.scan_tags(ScannedImage(im), md)

    def scan_file(self, pathname: Path) -> Iterable[arkimet.Metadata]:
        """Scan a JPEG file."""
        md = arkimet.Metadata.for_path("jpeg", pathname)
        with Image.open(pathname) as im:
            self.scan_image(im, md)
        yield md

    def scan_pipe(self, file: IO[bytes]) -> Iterable[arkimet.Metadata]:
        """Scan JPEG data from a pipe."""
        yield self.scan_data(file.read())

    def scan_data(self, data: bytes) -> arkimet.Metadata:
        """Scan a data buffer as JPEG."""
        md = arkimet.Metadata.for_bytes("jpeg", data)
        with io.BytesIO(data) as fd:
            with Image.open(fd) as im:
                self.scan_image(im, md)
        return md

    def scan_tags(self, sample: ScannedImage, md: arkimet.Metadata) -> None:
        """Scan EXIF tags."""
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

    def register(
        self,
        scanner: Callable[[ScannedImage, arkimet.Metadata], None],
        priority: int = 0,
    ) -> None:
        """
        Register a callable as an additional scanner.

        :param scanner: the callable to run to scan extra data from an image
        :param priority: the order of execution (lower numbers run first)
        """
        if scanner not in self.scanners:
            self.scanners.append((priority, scanner))
            self.scanners_sorted = False


#: Main JPEGScanner instance that handles scanning, and that plugins can use to
#: register themselves
jpeg_scanner = JPEGScanner()

# Register jpeg_scanner with the arkimet library
arkimet.scan.registry.register_scanner("jpeg", jpeg_scanner)
