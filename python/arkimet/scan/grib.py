import logging
from collections import defaultdict
from pathlib import Path
from typing import Callable, Dict, List, Tuple, Iterable, IO

import _arkimet

import arkimet

Grib = _arkimet.scan.grib.Grib
GribReader = _arkimet.scan.grib.GribReader

log = logging.getLogger("arkimet.scan.grib")


class Scanner:
    """
    Legacy python engine for scanning GRIB files.

    This is a proxy for GRIBScanner, used so that legacy scanner plugins can
    register themselves.
    """

    #
    #     #: Registered scanner plugins by GRIB edition number
    #     by_edition: Dict[
    #         int, List[Tuple[int, Callable[[Grib, arkimet.Metadata], None]]]
    #     ] = defaultdict(list)
    #
    #     def __init__(self) -> None:
    #         """
    #         Initialize the scanner.
    #
    #         This is called once, at library startup.
    #
    #         Plugins need to already be registered by the time this is called.
    #         """
    #         for type, scanners in self.by_edition.items():
    #             scanners.sort(key=lambda p: p[0])
    #
    #     def scan(self, grib: Grib, md: arkimet.Metadata) -> None:
    #         """Scan a GRIB message."""
    #         # Find the formatter list for this style
    #         scanners = self.by_edition.get(grib.edition)
    #         if scanners is None:
    #             return None
    #
    #         # Try all scanner functions in the list, in priority order, stopping at
    #         # the first one that returns False.
    #         # Note that False is explicitly required: returning None will not stop
    #         # the scanner chain.
    #         for prio, scan in scanners:
    #             try:
    #                 if scan(grib, md) is False:
    #                     break
    #             except Exception:
    #                 log.warning("scanner function failed", exc_info=True)

    @classmethod
    def register(
        cls,
        edition: int,
        scanner: Callable[[Grib, arkimet.Metadata], None],
        priority=0,
    ) -> None:
        """Register a scanner plugin."""
        grib_scanner.register(edition, scanner, priority)


class GRIBScanner(arkimet.scan.DataFormatScanner):
    """
    Scanner for GRIB files.

    This is registered with the arkimet library as the entry point for managing
    GRIB data.

    Scanner configuration scripts for GRIB files need to register with this
    class to be executed.
    """

    def __init__(self) -> None:
        """Initialize the GRIBScanner with an empty plugin list."""
        #: Registered scanner plugins by GRIB edition number
        self.by_edition: Dict[
            int, List[Tuple[int, Callable[[Grib, arkimet.Metadata], None]]]
        ] = defaultdict(list)
        self.scanners_sorted: bool = True

    def get_scanners(
        self, edition: int
    ) -> List[Tuple[int, Callable[[Grib, arkimet.Metadata], None]]] | None:
        if not self.scanners_sorted:
            for scanners in self.by_edition.values():
                scanners.sort(key=lambda s: s[0])
        return self.by_edition.get(edition)

    def scan_grib(self, grib: Grib, md: arkimet.Metadata) -> None:
        """Scan a GRIB image as a Grib instance."""
        # Find the formatter list for this style
        scanners = self.get_scanners(grib.edition)
        if scanners is None:
            return

        # Try all scanner functions in the list, in priority order, stopping at
        # the first one that returns False.
        # Note that False is explicitly required: returning None will not stop
        # the scanner chain.
        for prio, scan in scanners:
            try:
                if scan(grib, md) is False:
                    break
            except Exception:
                log.warning("scanner function failed", exc_info=True)

    def scan_file(self, path: Path) -> Iterable[arkimet.Metadata]:
        """Scan a GRIB file."""
        with GribReader.from_path(path) as reader:
            for grib, md in reader:
                with grib:
                    self.scan_grib(grib, md)
                    yield md

    def scan_pipe(self, file: IO[bytes]) -> Iterable[arkimet.Metadata]:
        """Scan GRIB data from a pipe."""
        with GribReader.from_pipe(file) as reader:
            for grib, md in reader:
                with grib:
                    self.scan_grib(grib, md)
                    yield md

    def scan_data(self, data: bytes) -> arkimet.Metadata:
        """Scan a data buffer as GRIB."""
        with GribReader.from_bytes(data) as reader:
            for grib, md in reader:
                with grib:
                    self.scan_grib(grib, md)
                    return md

    def register(
        self,
        edition: int,
        scanner: Callable[[Grib, arkimet.Metadata], None],
        priority: int = 0,
    ) -> None:
        """
        Register a callable as an additional scanner.

        :param scanner: the callable to run to scan extra data from an image
        :param priority: the order of execution (lower numbers run first)
        """
        if scanner not in self.by_edition[edition]:
            self.by_edition[edition].append((priority, scanner))
            self.scanners_sorted = False


#: Main GRIBScanner instance that handles scanning, and that plugins can use to
#: register themselves
grib_scanner = GRIBScanner()

# Register grib the arkimet library
arkimet.scan.registry.register_scanner("grib", grib_scanner)
