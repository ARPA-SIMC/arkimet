from arkimet.formatter import Formatter
from arkimet.formatter.eccodes import GribTable
import arkimet as arki
import os
import re

re_grib1_product = re.compile(r"\*\*")


def format_product(v):
    """
    Format a product: return a string, or nil to fall back to other formatters
    """
    if v["style"] == "GRIB1":
        origin, product = v["origin"], v["product"]
        if origin == 200 or origin == 80:
            origin = 0

        names = GribTable.load(1, "2.{}.{}".format(origin, v["table"]))
        if names.has(product):
            return re_grib1_product.sub("^", names.desc(product))

        return None
    elif v["style"] == "GRIB2":
        centre, category, discipline, number, table_version, local_table_version = (
                v["centre"], v["category"], v["discipline"], v["number"],
                v["table_version"], v["local_table_version"])

        tableprefix = GribTable.get_grib2_table_prefix(centre, table_version, local_table_version)
        tablename = os.path.join(tableprefix, "4.2.{}.{}".format(discipline, category))
        names = GribTable.load(2, tablename)

        if names.has(number):
            return re_grib1_product.sub("^", names.desc(number))
    elif v["style"] == "BUFR":
        return None
    elif v["style"] == "VM2":
        s = "id={}".format(v["id"])
        product = arki.scan.vm2.get_variable(v["id"])
        for k, v in product.items():
            s += ", {}={}".format(k, v)
        return s
    else:
        return None


Formatter.register("product", format_product)
