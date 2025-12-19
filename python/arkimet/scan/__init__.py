import importlib
import importlib.util
import logging
import pkgutil
import sys
from pathlib import Path

import _arkimet

Scanner = _arkimet.scan.Scanner
data_formats = _arkimet.scan.data_formats

log = logging.getLogger("arkimet.scan")


class Registry:
    @classmethod
    def init_module(cls, spec, mod) -> None:
        try:
            spec.loader.exec_module(mod)
        except Exception as e:
            log.error("%s: could not initialize module %s", spec.origin, mod.__name__, exc_info=e)

    @classmethod
    def load_arkimet(cls) -> None:
        for data_format in data_formats:
            try:
                importlib.import_module("arkimet.scan." + data_format)
            except ModuleNotFoundError:
                pass

    @classmethod
    def load_path(cls, path: Path, namespace: str) -> None:
        try:
            orig_path = sys.path
            sys.path = [path.as_posix()] + sys.path
            for data_format in data_formats:
                spec = importlib.util.find_spec(data_format)
                if spec is not None:
                    mod = importlib.util.module_from_spec(spec)
                    cls.init_module(spec, mod)
                    if spec.origin is None and data_format in ("grib", "bufr"):
                        # Special case grib and bufr namespace packages for
                        # compatibility
                        for module_info in pkgutil.iter_modules([path / data_format]):
                            subspec = importlib.util.find_spec(data_format + "." + module_info.name)
                            if subspec is not None:
                                submod = importlib.util.module_from_spec(subspec)
                                cls.init_module(subspec, submod)
        finally:
            sys.path = orig_path

    @classmethod
    def load(cls) -> None:
        cls.load_arkimet()
        for idx, path in enumerate(_arkimet.config()["scan"]["dirs"]):
            cls.load_path(path, "arkimet.scan.conf" + str(idx))


Registry.load()


__all__ = ("Scanner", "data_formats")
