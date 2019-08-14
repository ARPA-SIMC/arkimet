from arkimet.scan.bufr import Scanner, read_area_fixed, read_proddef

pollution_codes = {
    "B15192": "NO",
    "B15193": "NO2",
    "B15194": "O3",
    "B15195": "PM10",
    "B15196": "CO",
    "B15197": "SO2",
    "B15198": "PM2.5",
}


def scan(msg, md):
    area = read_area_fixed(msg)
    if area:
        md["area"] = {"style": "GRIB", "value": area}

    proddef = read_proddef(msg)
    gems = msg.get(None, None, 'B01214')
    name = msg.get(None, None, 'B01019')
    if gems or name:
        if proddef is None:
            proddef = {}
        if gems:
            proddef["gems"] = gems.enqc()
        if name:
            proddef["name"] = name.enqc()
    if proddef:
        md["proddef"] = {"style": "GRIB", "value": proddef}

    # Look for pollutant type
    polltype = msg.get_pollution_type()
    if polltype is not None:
        polltype = pollution_codes.get(polltype)
    if polltype is not None:
        product = md.to_python("product")
        if product is None:
            raise RuntimeError("cannot add pollution type to a missing product")
        value = product.get("value")
        if value is None:
            value = {}
            product["value"] = value
        value["p"] = polltype
        md["product"] = product


Scanner.register("pollution", scan)
