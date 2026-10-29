import logging
import tempfile
from pathlib import Path
from typing import Callable, List, Tuple, Iterable, IO

import netCDF4

import arkimet
import arkimet.scan

log = logging.getLogger("arkimet.scan.netcdf")


class NetCDFScanner(arkimet.scan.DataFormatScanner):
    """
    Scanner for NetCDF files.

    This is registered with the arkimet library as the entry point for managing
    NetCDF data.

    Scanner configuration scripts for NetCDF files need to register with this
    class to be executed.
    """

    def __init__(self) -> None:
        """Intialize the NetCDF scanner with an empty plugin list."""
        self.scanners: List[
            Tuple[int, Callable[["netCDF4.Dataset", arkimet.Metadata], None]]
        ] = []
        self.scanners_sorted: bool = True

    def scan_single_file(self, pathname: Path, md: arkimet.Metadata) -> None:
        """Scan a NetCDF file."""
        with netCDF4.Dataset(pathname, "r") as f:
            # Try all scanner functions in the list, in priority order,
            # stopping at the first one that returns False.
            # Note that False is explicitly required: returning None will not
            # stop the scanner chain.
            for prio, scan in self.scanners:
                try:
                    if scan(f, md) is False:
                        break
                except Exception:
                    log.warning("scanner function failed", exc_info=True)
        return md

    def scan_file(self, pathname: Path) -> Iterable[arkimet.Metadata]:
        """Scan a NetCDF file."""
        md = arkimet.Metadata.for_path("netcdf", pathname)
        if not md.data_size:
            return
        self.scan_single_file(pathname, md)
        yield md

    def scan_pipe(self, file: IO[bytes]) -> Iterable[arkimet.Metadata]:
        """Scan NetCDF data from a pipe."""
        data = file.read()
        if not data:
            return
        yield self.scan_data(data)

    def scan_data(self, data: bytes) -> arkimet.Metadata:
        """Scan a bytes object as NetCDF."""
        md = arkimet.Metadata.for_bytes(data)
        with tempfile.NamedTemporaryFile("wb") as tf:
            tf.write(data)
            tf.flush()
            self.scan_single_file(Path(tf.name), md)
        return md

    def register(
        self,
        scanner: Callable[["netCDF4.Dataset", arkimet.Metadata], None],
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


#: Main NetCDFScanner instance that handles scanning, and that plugins can use
#: to register themselves
netcdf_scanner = NetCDFScanner()

# Register netcdf_scanner with the arkimet library
arkimet.scan.registry.register_scanner("netcdf", netcdf_scanner)
