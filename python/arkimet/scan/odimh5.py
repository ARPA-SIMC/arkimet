from typing import Callable
import arkimet
try:
    import h5py
    HAVE_H5PY = True
except ImportError:
    HAVE_H5PY = False
import logging

log = logging.getLogger("arkimet.scan.odimh5")


class Scanner:
    scanners = []

    def __init__(self):
        scanners.sort(key=lambda p: p[0])

    def scan(self, pathname: str, md: arkimet.Metadata):
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
                        log.exception("scanner function failed")
        else:
            import json
            import inspect
            import subprocess
            metadata = {}
            for prio, scan in self.scanners:
                scanner = inspect.getfile(scan)
                proc = subprocess.Popen(["python", scanner, pathname],
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        universal_newlines=True)
                stdout, stderr = proc.communicate(input=json.dumps(metadata))
                if proc.returncode != 0:
                    log.error("scanner %s returned error code %d", scanner, proc.returncode)
                    continue

                metadata = json.loads(stdout)

            for k, v in metadata.items():
                md[k] = v

    @classmethod
    def register(cls, scanner: Callable[["h5py.File", arkimet.Metadata], None], priority=0):
        if scanner not in cls.scanners:
            cls.scanners.append((priority, scanner))
