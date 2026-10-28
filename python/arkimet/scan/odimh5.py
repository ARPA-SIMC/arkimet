import logging
import sys
import tempfile
from pathlib import Path
from typing import Callable, List, Tuple, Any, Dict

import arkimet
import arkimet.scan

import h5py

log = logging.getLogger("arkimet.scan.odimh5")


class ODIMH5Scanner(arkimet.scan.DataFormatScanner):
    """
    Scanner for ODIM HDF5 files.

    This is registered with the arkimet library as the entry point for managing
    ODIM data.

    Scanner configuration scripts for ODIM files need to register with this
    class to be executed.
    """

    def __init__(self) -> None:
        """Intialize the ODIM scanner with an empty plugin list."""
        self.scanners: List[
            Tuple[int, Callable[[h5py.File, arkimet.Metadata], None]]
        ] = []
        self.scanners_sorted: bool = True

    def scan_data(self, data: bytes, md: arkimet.Metadata):
        """Scan a memory buffer as an ODIM file."""
        with tempfile.NamedTemporaryFile("wb") as tf:
            tf.write(data)
            tf.flush()
            self.scan_file(Path(tf.name), md)

    def scan_file(self, pathname: Path, md: arkimet.Metadata) -> None:
        """Scan an ODIM file."""
        with h5py.File(pathname, "r") as f:
            # Try all scanner functions in the list, in priority order,
            # stopping at the first one that returns False.
            # Note that False is explicitly required: returning None will
            # not stop the scanner chain.
            for prio, scan in self.scanners:
                try:
                    if scan(f, md) is False:
                        break
                except Exception:
                    log.warning("scanner function failed", exc_info=True)

    def register(
        self,
        scanner: Callable[[h5py.File, arkimet.Metadata], None],
        priority: int = 0,
    ):
        """
        Register a callable as an additional scanner.

        :param scanner: the callable to run to scan extra data from an image
        :param priority: the order of execution (lower numbers run first)
        """
        if scanner not in self.scanners:
            self.scanners.append((priority, scanner))
            self.scanners_sorted = False


#: Main ODIMH5Scanner instance that handles scanning, and that plugins can use
#: to register themselves
odimh5_scanner = ODIMH5Scanner()

# Register odimh5_scanner with the arkimet library
arkimet.scan.registry.register_scanner("odimh5", odimh5_scanner)
