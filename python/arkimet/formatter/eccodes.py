import os
import re


def get_eccodes_def_dir() -> str:
    """
    get the list of directories (separated by :) where grib_api/eccodes keep their definitions
    """
    path = os.environ.get("ECCODES_DEFINITION_PATH", None)
    if path is not None:
        return path.split(":")

    path = os.environ.get("GRIBAPI_DEFINITION_PATH", None)
    if path is not None:
        return path.split(":")

    return ["/usr/share/eccodes/definitions/"]


class GribTable:
    """
    Read a grib table.

    edition is the GRIB edition: 1 or 2

    table is the table name, for example "0.0"

    Returns a table where the index maps to a couple { abbreviation, description },
    or nil if the file had no such entry.

    For convenience, the table has also two functions, 'abbr' and 'desc', that
    return the abbreviation or the description, falling back on returning the table
    index if they are not available.

    For example:

    origins = GribTable(1, "0")
    print(origins.abbr(98))  -- Prints 'ecmf'
    print(origins.desc(98))  -- Prints 'European Center for Medium-Range Weather Forecasts'
    print(origins.abbr(999)) -- Prints '999'
    print(origins.desc(999)) -- Prints '999'
    """

    cache = {}
    re_table_line = re.compile(r"^\s*(?P<idx>\d+)\s+(?P<abbr>\S+)\s+(?P<desc>.+)$")

    def __init__(self, edition: int, table: str):
        self.edition = edition
        self.table = table
        self._abbr = {}
        self._desc = {}

        for path in get_eccodes_def_dir():
            # Build the file name
            fname = os.path.join(path, "grib" + str(edition), str(table)) + ".table"
            try:
                with open(fname, "rt") as fd:
                    for line in fd:
                        mo = self.re_table_line.match(line)
                        if not mo:
                            continue

                        idx = int(mo.group("idx"))
                        self._abbr[idx] = mo.group("abbr")
                        self._desc[idx] = mo.group("desc").strip()
            except FileNotFoundError:
                pass

    def set(self, code: int, abbr: str, desc: str):
        """
        Add/replace a value in the table
        """
        self._abbr[code] = abbr
        self._desc[code] = desc

    def has(self, val: int) -> bool:
        return val in self._abbr

    def abbr(self, val: int) -> str:
        """
        Get an abbreviated description
        """
        res = self._abbr.get(val)
        if res is None:
            return str(val)
        else:
            return res

    def desc(self, val: int) -> str:
        """
        Get a long description
        """
        res = self._desc.get(val)
        if res is None:
            return str(val)
        else:
            return res

    @classmethod
    def load(cls, edition: int, table: str) -> "GribTable":
        key = (edition, table)
        res = cls.cache.get(key)
        if res is None:
            res = cls(edition, table)
            cls.cache[key] = res
        return res

    @classmethod
    def get_grib2_table_prefix(cls, centre, table_version, local_table_version):
        default_table_version = 4

        if table_version is None or table_version == 255:
            table_version = default_table_version

        if local_table_version is not None and local_table_version not in (0, 255):
            centres = cls.load(1, "0")
            if centres.has(centre):
                return os.path.join('tables', 'local', centres.abbr(centre), str(local_table_version))

        return os.path.join('tables', str(table_version))
