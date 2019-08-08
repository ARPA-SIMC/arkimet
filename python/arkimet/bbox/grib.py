from arkimet.bbox import BBox
from .common import utm2ll2, utm2ll, norm_lon
import math


def bbox_grib(area):
    """
    Compute bounding boxes for GRIB areas
    """
    v = area["value"]
    if v.get("utm") == 1 and v.get("tn") == 32768:
        latfirst, latlast, lonfirst, lonlast = (
                v["latfirst"], v["latlast"], v["lonfirst"], v["lonlast"])
        fe, fn, zone = v["fe"], v["fn"], v["zone"]

        # Compute the bounding box for the Calmet gribs, that memorize the UTM
        # coordinates instead of lat and lon
        bbox = []
        bbox.append(utm2ll2(lonfirst, latfirst, fe, fn, zone))
        bbox.append(utm2ll2(lonlast,  latfirst, fe, fn, zone))
        bbox.append(utm2ll2(lonlast,  latlast, fe, fn, zone))
        bbox.append(utm2ll2(lonfirst, latlast, fe, fn, zone))
        bbox.append(bbox[0])
        return bbox
    elif v.get("utm") == 1:
        latfirst = v["latfirst"] / 1000.0
        latlast = v["latlast"] / 1000.0
        lonfirst = norm_lon(v["lonfirst"] / 1000.0)
        lonlast = norm_lon(v["lonlast"] / 1000.0)

        # Compute the bounding box for the Calmet gribs, that memorize the UTM
        # coordinates instead of lat and lon
        bbox = []
        bbox.append(utm2ll(lonfirst, latfirst))
        bbox.append(utm2ll(lonlast,  latfirst))
        bbox.append(utm2ll(lonlast,  latlast))
        bbox.append(utm2ll(lonfirst, latlast))
        bbox.append(bbox[0])
        return bbox
    elif (v.get("type") == 0 or v.get("tn") == 0) and v.get("latfirst") is not None:
        # Normal latlon
        latfirst = v["latfirst"] / 1000000.0
        latlast = v["latlast"] / 1000000.0
        lonfirst = norm_lon(v["lonfirst"] / 1000000.0)
        lonlast = norm_lon(v["lonlast"] / 1000000.0)

        bbox = []
        bbox.append((latfirst, lonfirst))
        bbox.append((latlast,  lonfirst))
        bbox.append((latlast,  lonlast))
        bbox.append((latfirst, lonlast))
        bbox.append(bbox[0])
        return bbox
    elif ((v.get("type") == 10 or v.get("tn") == 1)
          and v.get("latp") is not None and v.get("lonp") is not None
          and v.get("rot") == 0):
        # Rotated latlon
        # ProjectionInDegrees è grib2
        latsp = v["latp"] / 1000000.0
        lonsp = v["lonp"] / 1000000.0

        # Common parameters to unrotate coordinates
        latsouthpole = math.acos(-math.sin(math.radians(latsp)))
        cy0 = math.cos(latsouthpole)
        sy0 = math.sin(latsouthpole)
        lonsouthpole = lonsp

        def r2ll(x, y):
            x = math.radians(x)
            y = math.radians(y)
            rlat = math.asin(sy0 * math.cos(y) * math.cos(x) + cy0 * math.sin(y))
            lat = math.degrees(rlat)
            lon = lonsouthpole + math.degrees(math.asin(math.sin(x) * math.cos(y) / math.cos(rlat)))
            return (lat, lon)

        # Number of points to sample
        xsamples = math.floor(math.log(v["Ni"]))
        ysamples = math.floor(math.log(v["Nj"]))

        latmin = v["latfirst"] / 1000000.0
        latmax = v["latlast"] / 1000000.0
        lonmin = v["lonfirst"] / 1000000.0
        lonmax = v["lonlast"] / 1000000.0
        if lonmax < lonmin:
            lonsize = lonmax + 360 - lonmin
        else:
            lonsize = lonmax - lonmin

        bbox = []

        for i in range(xsamples + 1):
            bbox.append(r2ll(lonmin + lonsize * i / xsamples, latmin))

        for i in range(ysamples + 1):
            bbox.append(r2ll(lonmax, latmin + (latmax - latmin) * i / ysamples))

        for i in range(xsamples, -1, -1):
            bbox.append(r2ll(lonmin + lonsize * i / xsamples, latmax))

        for i in range(ysamples, -1, -1):
            bbox.append(r2ll(lonmin, latmin + (latmax - latmin) * i / ysamples))

        return bbox
    elif v.get("type") == "mob":
        x = v["x"]
        y = v["y"]
        return [
            (y, x),
            (y + 1, x),
            (y + 1, x + 1),
            (y, x + 1),
            (y, x),
        ]
    elif v.get("lat") is not None and v.get("lon") is not None:
        return [
            (v["lat"] / 100000, v["lon"] / 100000)
        ]


BBox.register("GRIB", bbox_grib)
