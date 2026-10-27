import logging
from collections import defaultdict
from typing import Callable, Dict, List, Tuple

import _arkimet

import arkimet

Grib = _arkimet.scan.grib.Grib
GribReader = _arkimet.scan.grib.GribReader

log = logging.getLogger("arkimet.scan.grib")


class Scanner:
    """
    Python engine for scanning GRIB files.

    This is instantiated once during library initialization, and called to scan
    each GRIB message.

    It manages its own plugin system to be extended with scanner configuration
    scripts.
    """

    #: Registered scanner plugins by GRIB edition number
    by_edition: Dict[
        int, List[Tuple[int, Callable[[Grib, arkimet.Metadata], None]]]
    ] = defaultdict(list)

    def __init__(self) -> None:
        """
        Initialize the scanner.

        This is called once, at library startup.

        Plugins need to already be registered by the time this is called.
        """
        for type, scanners in self.by_edition.items():
            scanners.sort(key=lambda p: p[0])

    def scan(self, grib: Grib, md: arkimet.Metadata) -> None:
        """Scan a GRIB message."""
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
                log.warning("scanner function failed", exc_info=True)

    @classmethod
    def register(
        cls,
        edition: int,
        scanner: Callable[[Grib, arkimet.Metadata], None],
        priority=0,
    ) -> None:
        """Register a scanner plugin."""
        if scanner not in cls.by_edition[edition]:
            cls.by_edition[edition].append((priority, scanner))
