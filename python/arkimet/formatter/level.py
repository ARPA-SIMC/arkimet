from arkimet.formatter import Formatter
from arkimet.formatter.eccodes import GribTable
import os


def format_level(v):
    if v["style"] == "GRIB1":
        type, l1, l2 = v["level_type"], v.get("l1"), v.get("l2")
        levels = GribTable.load(1, "3")
        if levels.has(type):
            if l1 is None:
                l1 = "-"
            if l2 is None:
                l2 = "-"
            return "{} {} {} {}".format(
                                 levels.abbr(type),
                                 levels.desc(type),
                                 l1, l2)
        else:
            return None
    elif v["style"] in ("GRIB2S", "GRIB2D"):
        # TODO: index centre, table_version, local_table_version
        centre, table_version, local_table_version = None, None, None

        if v["style"] == "GRIB2S":
            type1, scale1, value1 = v["level_type"], v["scale"], v["value"]
            type2, scale2, value2 = None, None, None
        else:
            type1, scale1, value1 = v["l1"], v["scale1"], v["value1"]
            type2, scale2, value2 = v["l2"], v["scale2"], v["value2"]

        levels = GribTable.load(
                2, os.path.join(GribTable.get_grib2_table_prefix(centre, table_version, local_table_version), "4.5"))

        def format_single_level(type, scale, value):
            if levels.has(type):
                if value is None:
                    value = "-"
                if scale is None:
                    scale = "-"
                return "{} {} {} {}".format(levels.abbr(type), levels.desc(type), scale, value)
            else:
                return None

        if type2 is None:
            return format_single_level(type1, scale1, value1)
        else:
            return format_single_level(type1, scale1, value1) + " " + format_single_level(type2, scale2, value2)


Formatter.register("level", format_level)
