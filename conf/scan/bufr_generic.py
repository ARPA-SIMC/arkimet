from arkimet.scan.bufr import Scanner, read_area_fixed, read_proddef
from arkimet.scan import timedef


def read_timerange(msg):
    # Set timerange
    p1_list = {d["trange"].p1 for d in msg.query_data()}
    if len(p1_list) > 1:
        raise Exception("BUFR message has contradictory forecasts {}".format(
            p1_list
        ))

    p1 = p1_list.pop() if p1_list else 0
    unit = timedef.UNIT_SECOND

    for d, u in (
        (3600, timedef.UNIT_HOUR),
        (60, timedef.UNIT_MINUTE),
    ):
        if p1 % d == 0:
            p1 = p1 // d
            unit = u
            break

    return {
        "style": "Timedef",
        "step_len": p1,
        "step_unit": unit,
    }

def is_generic(md):
    product = md.to_python("product")
    return product.get("type") == 255 and product.get("subtype") == 255


def scan(msg, md):
    # Set area and proddef
    area = read_area_fixed(msg)
    proddef = read_proddef(msg)

    if area:
        md["area"] = {"style": "GRIB", "value": area}

    if proddef:
        md["proddef"] = {"style": "GRIB", "value": proddef}

    # Set product based on report name
    if msg.report:
        product = md.to_python("product")
        product["value"]["t"] = msg.report
        md["product"] = product

    md["timerange"] = read_timerange(msg)


def scan_temp(msg, md):
    # Add timerange to generic with rep_memo=temp
    if is_generic(md):
        md["timerange"] = read_timerange(msg)


Scanner.register("generic", scan)
Scanner.register("temp", scan_temp, 1)
