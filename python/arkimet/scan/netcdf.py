import logging
import tempfile
from pathlib import Path
from typing import Callable

import netCDF4

import arkimet
import arkimet.scan

log = logging.getLogger("arkimet.scan.netcdf")


class NetCDFScanner(arkimet.scan.DataFormatScannner):
    def __init__(self):
        self.scanners = []
        self.scanners_sorted = True

    def scan_data(self, data: bytes, md: arkimet.Metadata):
        with tempfile.NamedTemporaryFile("wb") as tf:
            tf.write(data)
            tf.flush()
            self.scan_file(Path(tf.name), md)

    def scan_file(self, pathname: Path, md: arkimet.Metadata) -> None:
        with netCDF4.Dataset(pathname, "r") as f:
            # Try all scanner functions in the list, in priority order, stopping at
            # the first one that returns False.
            # Note that False is explicitly required: returning None will not stop
            # the scanner chain.
            for prio, scan in self.scanners:
                try:
                    if scan(f, md) is False:
                        break
                except Exception:
                    log.warning("scanner function failed", exc_info=True)

    def register(self, scanner: Callable[["netCDF4.Dataset", arkimet.Metadata], None], priority=0) -> None:
        if scanner not in self.scanners:
            self.scanners.append((priority, scanner))
            self.scanners_sorted = False


netcdf_scanner = NetCDFScanner()

arkimet.scan.registry.register_scanner("netcdf", netcdf_scanner)
