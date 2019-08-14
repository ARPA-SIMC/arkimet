from arkimet.scan.bufr import Scanner, read_area_fixed, read_proddef


def scan(msg, md):
    area = read_area_fixed(msg)
    proddef = read_proddef(msg)
    if area:
        md["area"] = {"style": "GRIB", "value": area}
    if proddef:
        md["proddef"] = {"style": "GRIB", "value": proddef}


Scanner.register("metar", scan)
Scanner.register("buoy", scan)
Scanner.register("pilot", scan)
Scanner.register("synop", scan)
Scanner.register("temp", scan)
