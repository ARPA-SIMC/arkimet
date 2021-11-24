import datetime
from typing import TYPE_CHECKING, Dict, Any

from arkimet.scan.jpeg import Scanner

if TYPE_CHECKING:
    import arkimet


def scan_reftime(tags: Dict[str, Any], md: "arkimet.Metadata"):
    dt = None

    gps_date = tags.get("GPS GPSDate")
    gps_time = tags.get("GPS GPSTimeStampz")
    if gps_date is not None and gps_time is not None:
        str_date = str(gps_date)
        dt = datetime.datetime(
                int(str_date[:4], 10), int(str_date[5:7], 10), int(str_date[8:10], 10),
                int(gps_time.values[0]), int(gps_time.values[1]), int(gps_time.values[2]))
        md["reftime"] = {
            "style": "POSITION",
            "time": dt,
        }

    if dt is None:
        img_dt = tags.get("Image DateTime")
        if img_dt is not None:
            dt = datetime.datetime.strptime(str(img_dt), "%Y:%m:%d %H:%M:%S")

    if dt is not None:
        md["reftime"] = {
            "style": "POSITION",
            "time": dt,
        }


def scan_area(tags: Dict[str, Any], md: "arkimet.Metadata"):
    gps_lat = tags.get("GPS GPSLatitude")
    gps_lon = tags.get("GPS GPSLongitude")

    if gps_lat is None or gps_lon is None:
        return

    lat = round((gps_lat.values[0] + gps_lat.values[1] / 60 + gps_lat.values[2] / 3600) * 10000)
    lon = round((gps_lon.values[0] + gps_lon.values[1] / 60 + gps_lon.values[2] / 3600) * 10000)

    # TODO: figure the right area, this is a point as a collapsed rectangle!
    md["area"] = {"style": "GRIB", "value": {
            "type": 0,
            "latfirst": lat,
            "latlast": lat,
            "lonfirst": lon,
            "lonlast": lon,
        },
    }


def scan(tags: Dict[str, Any], md: "arkimet.Metadata"):
    scan_reftime(tags, md)
    scan_area(tags, md)

    # Uncomment this to print all available tags
    # Alternatively, use /usr/share/doc/python3-exif/examples/EXIF.py
    # Or see https://github.com/ianare/exif-py
    #
    # for k, v in tags.items():
    #     print(f"TAG {k!r}: {v!r}")


Scanner.register(scan)
