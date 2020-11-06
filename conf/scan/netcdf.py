from arkimet.scan.netcdf import Scanner
import netCDF4
import datetime

POSSIBLE_TIME_DIMENSIONS = ("dstart", "time", "ocean_time")


def scan_area_default(dataset, md):
    """
    If lat and lon dimensions are available, set area to their bounding box
    """
    if "lat" not in dataset.dimensions or "lon" not in dataset.dimensions:
        return

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


def scan_reftime_default(dataset, md):
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


def scan_product_default(dataset, md):
    # TODO: use product instead
    md["quantity"] = {"value": list(dataset.variables)}


def scan_arpa(dataset, md):
    # TODO: What do we use as origin?
    md["origin"] = {"style": "GRIB1", "centre": 255, "subcentre": 255, "process": 255}

    region = getattr(dataset, "region", None)
    if region is not None:
        # TODO: hardcode the area to use for the adriatic sea
        md["area"] = {"style": "GRIB", "value": {}}
    else:
        scan_area_default(dataset, md)

    runtype = getattr(dataset, "runtype", None)
    if runtype in ("fc", "forecast"):
        forecast_reference_time = getattr(dataset, "forecast_reference_time", None)
        if forecast_reference_time is not None:
            dt = datetime.datetime.strptime(forecast_reference_time, "%Y-%m-%d %H:%M:%S")
            md["reftime"] = {
                "style": "POSITION",
                "time": dt,
            }
        else:
            scan_reftime_default(dataset, md)
    else:
        scan_reftime_default(dataset, md)

    scan_product_default(dataset, md)


def scan(dataset, md):
    conventions = getattr(dataset, "Conventions", None)
    if conventions is None:
        return
    if not conventions.startswith("CF-"):
        return
    # print(dataset.dimensions)

    institution = getattr(dataset, "institution", None)
    if institution == "Arpae-Simc":
        scan_arpa(dataset, md)
    else:
        scan_area_default(dataset, md)
        scan_reftime_default(dataset, md)
        scan_product_default(dataset, md)

    # DRAFT: Origin
    # DRAFT: Area
    # DRAFT: Reftime
    # DRAFT: Product
    # TODO: Level
    # TODO: Timerange


Scanner.register(scan)
