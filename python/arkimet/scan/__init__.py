import abc
import importlib
import importlib.util
import logging
import pkgutil
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import _arkimet

if TYPE_CHECKING:
    import arkimet

Scanner = _arkimet.scan.Scanner
data_formats = _arkimet.scan.data_formats

log = logging.getLogger("arkimet.scan")


class Registry:
    """
    Scanner registry.
    """

    def __init__(self) -> None:
        self.loaded: bool = False

    def init_module(self, spec, mod) -> None:
        try:
            spec.loader.exec_module(mod)
        except Exception as e:
            log.error("%s: could not initialize module %s", spec.origin, mod.__name__, exc_info=e)

    def load_arkimet(self) -> None:
        for data_format in data_formats:
            try:
                importlib.import_module("arkimet.scan." + data_format)
            except ModuleNotFoundError:
                pass

    def load_path(self, path: Path, namespace: str) -> None:
        try:
            orig_path = sys.path
            sys.path = [path.as_posix()] + sys.path
            for data_format in data_formats:
                spec = importlib.util.find_spec(data_format)
                if spec is not None:
                    mod = importlib.util.module_from_spec(spec)
                    self.init_module(spec, mod)
                    if spec.origin is None and data_format in ("grib", "bufr"):
                        # Special case grib and bufr namespace packages for
                        # compatibility
                        for module_info in pkgutil.iter_modules([path / data_format]):
                            subspec = importlib.util.find_spec(data_format + "." + module_info.name)
                            if subspec is not None:
                                submod = importlib.util.module_from_spec(subspec)
                                self.init_module(subspec, submod)
        finally:
            sys.path = orig_path

    def load(self) -> None:
        self.load_arkimet()
        for idx, path in enumerate(_arkimet.config()["scan"]["dirs"]):
            self.load_path(path, "arkimet.scan.conf" + str(idx))
        self.loaded = True

    def ensure_loaded(self) -> None:
        if not self.loaded:
            self.load()

    def register_scanner(self, data_format: str, scanner: "DataFormatScanner") -> None:
        Scanner.register_scanner(data_format, scanner)


class DataFormatScannner(abc.ABC):
    """Interface for data format scanners."""

    @abc.abstractmethod
    def scan_file(self, path: Path, md: "arkimet.Metadata") -> None:
        """Scan metadata for the given file."""

    @abc.abstractmethod
    def scan_data(self, data: bytes, md: "arkimet.Metadata") -> None:
        """Scan metadata for the given file."""


registry = Registry()


__all__ = ("Scanner", "data_formats")
