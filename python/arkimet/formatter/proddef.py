from arkimet.formatter import Formatter


def format_proddef(v):
    if v["style"] == "GRIB":
        # values = v.val
        return None
    else:
        return None


Formatter.register("proddef", format_proddef)
