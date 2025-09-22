import datetime
import json
from typing import TYPE_CHECKING

from arkimet.scan.jpeg import Scanner, ScannedImage
from arkimet.scan import timedef

if TYPE_CHECKING:
    import arkimet


def from_decimal(val):
    if isinstance(val, tuple):
        return val[0] / val[1]
    else:
        return val


def scan_reftime(sample: ScannedImage, md: "arkimet.Metadata"):
    dt = None

    gps_date = sample.get_gps("GPSDateStamp")
    gps_time = sample.get_gps("GPSTimeStamp")
    if gps_date is not None and gps_time is not None:
        str_date = str(gps_date)
        dt = datetime.datetime(
            int(str_date[:4], 10),
            int(str_date[5:7], 10),
            int(str_date[8:10], 10),
            int(from_decimal(gps_time[0])),
            int(from_decimal(gps_time[1])),
            int(from_decimal(gps_time[2])),
        )
        md["reftime"] = {
            "style": "POSITION",
            "time": dt,
        }

    if dt is None:
        img_dt = sample.get_image("DateTime")
        if img_dt is not None:
            dt = datetime.datetime.strptime(str(img_dt), "%Y:%m:%d %H:%M:%S")

    if dt is not None:
        md["reftime"] = {
            "style": "POSITION",
            "time": dt,
        }


def scan_area(sample: ScannedImage, md: "arkimet.Metadata"):
    gps_lat_ref = sample.get_gps("GPSLatitudeRef")
    gps_lon_ref = sample.get_gps("GPSLongitudeRef")

    gps_lat = sample.get_gps("GPSLatitude")
    gps_lon = sample.get_gps("GPSLongitude")

    if gps_lat is None or gps_lon is None:
        return

    lat = round((from_decimal(gps_lat[0]) + from_decimal(gps_lat[1]) / 60 + from_decimal(gps_lat[2]) / 3600) * 10000)
    lon = round((from_decimal(gps_lon[0]) + from_decimal(gps_lon[1]) / 60 + from_decimal(gps_lon[2]) / 3600) * 10000)

    if gps_lat_ref == "S":
        lat = -lat
    if gps_lon_ref == "W":
        lon = -lon

    md["area"] = {
        "style": "GRIB",
        "value": {
            "lat": lat,
            "lon": lon,
        },
    }


def scan_usercomment(sample: ScannedImage, md: "arkimet.Metadata"):
    user_comment = sample.get_usercomment()
    if user_comment is None:
        return
    try:
        _, data = user_comment.split("{%", 1)
        data, _ = data.rsplit("%}", 1)
    except ValueError:
        return
    decoded = json.loads(data)
    level = decoded.get("level")
    if level is not None:
        ltype1, l1, ltype2, l2 = level
        md["level"] = {
            "style": "GRIB2D",
            "l1": ltype1,
            "value1": l1,
            "scale1": 1,
            "l2": ltype2,
            "value2": l2,
            "scale2": 1,
        }
    timerange = decoded.get("timerange")
    if timerange is not None:
        pind, p1, p2 = timerange
        if pind is not None and p1 is not None and p2 is not None:
            md["timerange"] = {
                "style": "Timedef",
                "step_len": p1,
                "step_unit": timedef.UNIT_SECOND,
                "stat_type": pind,
                "stat_len": p2,
                "stat_unit": timedef.UNIT_SECOND,
            }


def scan(sample: ScannedImage, md: "arkimet.Metadata"):
    # Uncomment this to see all exif tags available in the image
    # sample.dump()

    scan_reftime(sample, md)
    scan_area(sample, md)
    scan_usercomment(sample, md)


Scanner.register(scan)
