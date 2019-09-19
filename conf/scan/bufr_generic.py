# This does not currently seem to be invoked
#
# function scan(msg, md)
#     local area = bufr_read_area_fixed(msg)
#     local proddef = bufr_read_proddef(msg)
#     local report = msg:find("rep_memo")
#     if area then md:set(arki_area.grib(area)) end
#     if proddef then md:set(arki_proddef.grib(proddef)) end
#     if report then md:set(md.product:addValues{t=report:enqc()}) end
#     md:set(bufr_scan_forecast(msg))
# end
from arkimet.scan.bufr import Scanner, read_area_fixed, read_proddef
from arkimet.scan import timedef


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
        # TODO (see #190) md["product"]["t"] = msg.report
        pass

    # Set timerange
    p1_list = [d["trange"].p1 for d in msg.query_data()]
    if len(p1_list) > 1:
        raise Exception("BUFR message has contradictory forecasts {}".format(
            p1_list
        ))

    p1 = p1_list[0] if p1_list else 0
    unit = timedef.UNIT_SECOND

    for d, u in (
        (3600, timedef.UNIT_HOUR),
        (60, timedef.UNIT_MINUTE),
    ):
        if p1 % d == 0:
            p1 = p1 // d
            unit = u
            break

    md["timerange"] = {
        "style": "Timedef",
        "step_len": p1,
        "step_unit": unit,
    }


Scanner.register("generic", scan)
