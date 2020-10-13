from arkimet.scan.netcdf import Scanner
import netCDF4


# source_wmo = re.compile("WMO:([^,]+)")
# source_rad = re.compile("RAD:([^,]+)")
# source_nod = re.compile("NOD:([^,]+)")
# source_plc = re.compile("PLC:([^,]+)")
#
#
# def odimh5_set_origin(h5f, md):
#     source = h5f["what"].attrs["source"].decode()
#
#     origin = {"style": "ODIMH5"}
#
#     mo = source_wmo.search(source)
#     origin["wmo"] = mo.group(1) if mo else ""
#
#     mo = source_rad.search(source)
#     origin["rad"] = mo.group(1) if mo else ""
#
#     mo = source_nod.search(source)
#     if mo:
#         origin["plc"] = mo.group(1)
#     else:
#         mo = source_plc.search(source)
#         origin["plc"] = mo.group(1) if mo else ""
#
#     md["origin"] = origin
#
#
# def odimh5_pvol_set_level(h5f, md):
#     emin = 360
#     emax = -360
#     for name, group in h5f.items():
#         if not name.startswith("dataset"):
#             continue
#
#         elangle = group["where"].attrs.get("elangle")
#         if elangle is None:
#             continue
#
#         if elangle > emax:
#             emax = elangle
#         if elangle < emin:
#             emin = elangle
#
#     md["level"] = {"style": "ODIMH5", "min": emin, "max": emax}
#
#
# def odimh5_set_quantity(h5f, md):
#     quantities = []
#     for name, ds in h5f.items():
#         if not name.startswith("dataset"):
#             continue
#
#         for gname, g in ds.items():
#             if not gname.startswith("data"):
#                 continue
#
#             quantity = g["what"].attrs.get("quantity")
#             if quantity is not None:
#                 quantities.append(quantity.decode())
#
#     md["quantity"] = {"value": quantities}

POSSIBLE_TIME_DIMENSIONS = ("dstart", "time", "ocean_time")


def scan(dataset, md):
    conventions = getattr(dataset, "Conventions", None)
    if conventions is None:
        return
    if not conventions.startswith("CF-"):
        return
    # print(dataset.dimensions)

    # TODO: Origin
    # TODO: Product
    # TODO: Level
    # TODO: Timerange

    # Reftime
    for name in POSSIBLE_TIME_DIMENSIONS:
        if name in dataset.variables:
            times = dataset["/" + name]
            if not times.shape or not times.shape[0]:
                continue
            dt = netCDF4.num2date(times[0], units=times.units, calendar=times.calendar)
            md["reftime"] = {
                "style": "POSITION",
                "time": dt,
            }
            break

    # Area
    if "lat" in dataset.dimensions and "lon" in dataset.dimensions:
        lats = dataset["/lat"]
        lons = dataset["/lon"]
        md["area"] = {"style": "GRIB", "value": {
                "type": 0,
                "latfirst": int(round(min(lats) * 1000000)),
                "latlast": int(round(max(lats) * 1000000)),
                "lonfirst": int(round(min(lons) * 1000000)),
                "lonlast": int(round(max(lons) * 1000000)),
            },
        }


Scanner.register(scan)
