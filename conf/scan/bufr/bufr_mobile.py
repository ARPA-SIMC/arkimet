from arkimet.scan.bufr import Scanner, read_area_mobile, read_proddef


def scan(msg, md):
    area = read_area_mobile(msg)
    proddef = read_proddef(msg)
    if area:
        md["area"] = {"style": "GRIB", "value": area}
    if proddef:
        md["proddef"] = {"style": "GRIB", "value": proddef}


Scanner.register("amdar", scan)
Scanner.register("acars", scan)
Scanner.register("airep", scan)
Scanner.register("ship", scan)
Scanner.register("temp_ship", scan)
