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

    def scan(self, pathname: str, md: arkimet.Metadata):
        # FIXME: hack to deal with Centos not having h5py for python3
        if HAVE_H5PY:
            with h5py.File(pathname, "r") as f:
                # Try all scanner functions in the list, returning the result of the
                # first that returns not-None.
                # Iterate in reverse order, so that scanner functions loaded later
                # (like from /etc) can be called earlier and fall back on the shipped
                # ones
                for scan in reversed(self.scanners):
                    try:
                        scan(f, md)
                    except Exception:
                        log.exception("scanner function failed")
        else:
            import json
            import inspect
            import subprocess
            metadata = {}
            for scan in reversed(self.scanners):
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
    def register(cls, scanner: Callable[["h5py.File", arkimet.Metadata], None]):
        if scanner not in cls.scanners:
            cls.scanners.append(scanner)
