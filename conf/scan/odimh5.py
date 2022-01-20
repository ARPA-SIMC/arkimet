import datetime
import re
import math
import calendar


def odimh5_datetime2epoch(date, time):
    return calendar.timegm((
        int(date[:4]),
        int(date[4:6]),
        int(date[6:8]),
        int(time[:2]),
        int(time[2:4]),
        int(time[4:6]),
    ))


def odimh5_set_reftime(h5f, md):
    what = h5f["what"]
    date = what.attrs["date"]
    time = what.attrs["time"]
    timefmt = "%Y%m%d%H%M%S" if len(time) == 6 else "%Y%m%d%H%M"

    md["reftime"] = {
        "style": "POSITION",
        "time": datetime.datetime.strptime((date + time).decode(), timefmt)
    }


source_wmo = re.compile("WMO:([^,]+)")
source_rad = re.compile("RAD:([^,]+)")
source_nod = re.compile("NOD:([^,]+)")
source_plc = re.compile("PLC:([^,]+)")


def odimh5_set_origin(h5f, md):
    source = h5f["what"].attrs["source"].decode()

    origin = {"style": "ODIMH5"}

    mo = source_wmo.search(source)
    origin["wmo"] = mo.group(1) if mo else ""

    mo = source_rad.search(source)
    origin["rad"] = mo.group(1) if mo else ""

    mo = source_nod.search(source)
    if mo:
        origin["plc"] = mo.group(1)
    else:
        mo = source_plc.search(source)
        origin["plc"] = mo.group(1) if mo else ""

    md["origin"] = origin


def odimh5_pvol_set_level(h5f, md):
    emin = 360
    emax = -360
    for name, group in h5f.items():
        if not name.startswith("dataset"):
            continue

        elangle = group["where"].attrs.get("elangle")
        if elangle is None:
            continue

        if elangle > emax:
            emax = elangle
        if elangle < emin:
            emin = elangle

    md["level"] = {"style": "ODIMH5", "min": emin, "max": emax}


def odimh5_set_quantity(h5f, md):
    quantities = []
    for name, ds in h5f.items():
        if not name.startswith("dataset"):
            continue

        for gname, g in ds.items():
            if not gname.startswith("data"):
                continue

            quantity = g["what"].attrs.get("quantity")
            if quantity is not None:
                quantities.append(quantity.decode())

    md["quantity"] = {"value": quantities}


def odimh5_pvol_set_area(h5f, md):
    lat = h5f["where"].attrs["lat"]
    lon = h5f["where"].attrs["lon"]
    radius = 0
    for name, group in h5f.items():
        if not name.startswith("dataset"):
            continue
        if "how" in group:
            radhor = group["how"].attrs.get("radhoriz")
            if radhor is not None and radhor > radius:
                radius = radhor

    md["area"] = {"style": "ODIMH5", "value": {
        "lat": int(round(lat * 1000000)),
        "lon": int(round(lon * 1000000)),
        "radius": int(round(radius * 1000)),
    }}


def odimh5_horizobj_set_area(h5f, md):
    attrs = h5f["where"].attrs
    lons = [attrs[x] for x in ("LL_lon", "LR_lon", "UL_lon", "UR_lon")]
    lats = [attrs[x] for x in ("LL_lat", "LR_lat", "UL_lat", "UR_lat")]
    md["area"] = {"style": "GRIB", "value": {
            "type": 0,
            "latfirst": int(math.floor(min(lats) * 1000000)),
            "latlast": int(math.floor(max(lats) * 1000000)),
            "lonfirst": int(math.floor(min(lons) * 1000000)),
            "lonlast": int(math.floor(max(lons) * 1000000)),
        },
    }


def odimh5_xsec_set_area(h5f, md):
    attrs = h5f["where"].attrs
    md["area"] = {
        "style": "GRIB",
        "value": {
            "type": 0,
            # FIXME: this should be round instead of floor, but the previous
            # Lua scanning script truncated instead of rounding, so I'll
            # maintain to have compatibility with already scanned data
            "latfirst": int(math.floor(attrs["start_lat"] * 1000000)),
            "latlast": int(math.floor(attrs["stop_lat"] * 1000000)),
            "lonfirst": int(math.floor(attrs["start_lon"] * 1000000)),
            "lonlast": int(math.floor(attrs["stop_lon"] * 1000000)),
        },
    }


def is_hvmi(h5f):
    dataset2 = h5f.get("dataset2")
    if dataset2 is None:
        return False
    if "product" not in dataset2["what"].attrs:
        return False

    dataset3 = h5f.get("dataset3")
    if dataset3 is None:
        return False
    if "product" not in dataset3["what"].attrs:
        return False

    return True


def scan(h5f, md):
    # conventions = h5f.attrs["Conventions"]

    # reftime
    odimh5_set_reftime(h5f, md)
    # origin
    odimh5_set_origin(h5f, md)

    obj = h5f["what"].attrs["object"]
    if obj == b"PVOL":
        # polar volume

        # product
        md["product"] = {"style": "ODIMH5", "object": "PVOL", "product": "SCAN"}
        # task
        md["task"] = {"value": h5f["how"].attrs["task"].decode()}
        # level
        odimh5_pvol_set_level(h5f, md)
        # quantity
        odimh5_set_quantity(h5f, md)
        # area
        odimh5_pvol_set_area(h5f, md)
    elif obj in (b"COMP", b"IMAGE"):
        dataset1 = h5f["dataset1"]

        # product
        product = dataset1["what"].attrs["product"].decode()
        md["product"] = {"style": "ODIMH5", "object": obj.decode(), "product": product}
        # task
        md["task"] = {"value": h5f["how"].attrs["task"].decode()}
        # quantity
        odimh5_set_quantity(h5f, md)
        # area
        odimh5_horizobj_set_area(h5f, md)

        if product in ("CAPPI", "PCAPPI"):
            height = dataset1["what"].attrs["prodpar"]
            md["level"] = {"style": "GRIB1", "level_type": 105, "l1": round(height)}
        elif product == "PPI":
            height = dataset1["what"].attrs["prodpar"]
            md["level"] = {"style": "ODIMH5", "min": height, "max": height}
        elif product == "RR":
            date1 = dataset1["what"].attrs["startdate"]
            time1 = dataset1["what"].attrs["starttime"]
            date2 = dataset1["what"].attrs["enddate"]
            time2 = dataset1["what"].attrs["endtime"]
            diff = odimh5_datetime2epoch(date2, time2) - odimh5_datetime2epoch(date1, time1)
            md["timerange"] = {
                "style": "Timedef",
                "step_len": 0,
                "step_unit": UNIT_SECOND,
                "stat_type": 1,
                "stat_len": diff,
                "stat_unit": UNIT_SECOND,
            }
        elif product == "VIL":
            prodpar = dataset1["what"].attrs["prodpar"]
            bottom, top = (int(x) for x in prodpar.split(b","))
            md["level"] = {"style": "GRIB1", "level_type": 106, "l1": round(top / 100), "l2": round(bottom / 100)}
        elif is_hvmi(h5f):
            md["product"] = {"style": "ODIMH5", "object": obj.decode(), "product": "HVMI"}
    elif obj == b"XSEC":
        # product
        product = h5f["dataset1"]["what"].attrs["product"]
        md["product"] = {"style": "ODIMH5", "object": "XSEC", "product": product.decode()}
        # task
        md["task"] = {"value": h5f["how"].attrs["task"].decode()}
        # quantity
        odimh5_set_quantity(h5f, md)
        # area
        odimh5_xsec_set_area(h5f, md)


# Hack to deal with Centos7 having h5py only for python2
if __name__ == "__main__":
    UNIT_SECOND = 13
    import argparse
    import h5py
    import json

    class Encoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, datetime.datetime):
                return [o.year, o.month, o.day, o.hour, o.minute, o.second]
            else:
                return json.JSONEncoder.default(self, o)

    parser = argparse.ArgumentParser(description="scan ODIMH5 files for arkimet")
    parser.add_argument("file", help="file to scan")
    args = parser.parse_args()
    with h5py.File(args.file) as f:
        metadata = {}
        scan(f, metadata)
        print(json.dumps(metadata, cls=Encoder))
else:
    from arkimet.scan.odimh5 import Scanner
    from arkimet.scan.timedef import UNIT_SECOND
    Scanner.register(scan)
