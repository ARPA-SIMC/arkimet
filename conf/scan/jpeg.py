import datetime
from typing import TYPE_CHECKING

from arkimet.scan.jpeg import Scanner, ScannedImage

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
                int(str_date[:4], 10), int(str_date[5:7], 10), int(str_date[8:10], 10),
                int(from_decimal(gps_time[0])), int(from_decimal(gps_time[1])), int(from_decimal(gps_time[2])))
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

    if gps_lat_ref == 'S':
        lat = -lat
    if gps_lon_ref == 'W':
        lon = -lon

    md["area"] = {"style": "GRIB", "value": {
            "lat": lat,
            "lon": lon,
        },
    }


def scan(sample: ScannedImage, md: "arkimet.Metadata"):
    # Uncomment this to see all exif tags available in the image
    # sample.dump()

    scan_reftime(sample, md)
    scan_area(sample, md)


Scanner.register(scan)
