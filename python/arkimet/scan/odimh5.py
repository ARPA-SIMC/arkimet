import io
import logging
import sys
import tempfile
from pathlib import Path
from typing import Callable

import arkimet
import arkimet.scan

try:
    import h5py

    HAVE_H5PY = True
except ImportError:
    HAVE_H5PY = False

log = logging.getLogger("arkimet.scan.odimh5")


class ODIMH5Scanner(arkimet.scan.DataFormatScannner):
    def __init__(self):
        self.scanners = []
        self.scanners_sorted = True

    def scan_data(self, data: bytes, md: arkimet.Metadata):
        with tempfile.NamedTemporaryFile("wb") as tf:
            tf.write(data)
            tf.flush()
            self.scan_file(Path(tf.name), md)

    def scan_file(self, pathname: Path, md: arkimet.Metadata) -> None:
        # FIXME: hack to deal with Centos not having h5py for python3
        if HAVE_H5PY:
            with h5py.File(pathname, "r") as f:
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
        else:
            import json
            import inspect
            import subprocess

            metadata = {}
            for prio, scan in self.scanners:
                scanner = inspect.getfile(scan)
                proc = subprocess.Popen(
                    [sys.executable, scanner, pathname],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    universal_newlines=True,
                )
                stdout, stderr = proc.communicate(input=json.dumps(metadata))
                if proc.returncode != 0:
                    log.error("scanner %s returned error code %d", scanner, proc.returncode)
                    continue

                metadata = json.loads(stdout)

            for k, v in metadata.items():
                md[k] = v

    def register(self, scanner: Callable[[Path, arkimet.Metadata], None], priority=0):
        if scanner not in self.scanners:
            self.scanners.append((priority, scanner))
            self.scanners_sorted = False


odimh5_scanner = ODIMH5Scanner()

arkimet.scan.registry.register_scanner("odimh5", odimh5_scanner)
