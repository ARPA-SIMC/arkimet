from typing import Callable, Any
import functools
import io
import logging
import sys

from PIL import Image, ExifTags
import arkimet

log = logging.getLogger("arkimet.scan.jpeg")

image_tags = {v: k for k, v in ExifTags.TAGS.items()}
gps_tags = {v: k for k, v in ExifTags.GPSTAGS.items()}
gpsinfo_tag = image_tags["GPSInfo"]


class ScannedImage:
    def __init__(self, im: Image):
        self.im = im

    @functools.cached_property
    def exif(self):
        return self.im.getexif()

    @functools.cached_property
    def gpsinfo(self):
        gpsinfo = self.exif.get(gpsinfo_tag)
        return {} if gpsinfo is None else gpsinfo

    def get_image(self, tag_name: str) -> Any:
        tagid = image_tags.get(tag_name)
        if tagid is None:
            raise KeyError(f"Exif tag {tag_name!r} is not available in PIL.ExifTags.TAG")
        return self.exif[tagid]

    def get_gps(self, tag_name: str) -> Any:
        tagid = gps_tags.get(tag_name)
        if tagid is None:
            raise KeyError(f"Exif tag {tag_name!r} is not available in PIL.ExifTags.GPSTAGS")
        return self.gpsinfo.get(tagid)

    def dump(self, file=None):
        if file is None:
            file = sys.stderr

        lines = []

        for tagid, val in self.exif.items():
            if tagid in ExifTags.TAGS:
                lines.append(f"Image:{ExifTags.TAGS[tagid]}: {val!r}")
            else:
                lines.append(f"Unknown:{tagid:x}: {val!r}")

        for tagid, val in self.gpsinfo.items():
            if tagid in ExifTags.GPSTAGS:
                lines.append(f"GPS:{ExifTags.GPSTAGS[tagid]}: {val!r}")
            else:
                lines.append(f"UnknownGPS:{tagid:x}: {val!r}")

        lines.sort()
        for line in lines:
            print(line, file=file)


class Scanner:
    scanners = []

    def __init__(self):
        self.scanners.sort(key=lambda p: p[0])

    def scan_image(self, im: Image, md: arkimet.Metadata):
        self.scan_tags(ScannedImage(im), md)

    def scan_file(self, pathname: str, md: arkimet.Metadata):
        with Image.open(pathname) as im:
            self.scan_image(im, md)

    def scan_data(self, data: bytes, md: arkimet.Metadata):
        with io.ByteIO(data) as fd:
            with Image.open(fd) as im:
                self.scan_image(im, md)

    def scan_tags(self, sample: ScannedImage, md: arkimet.Metadata):
        # Try all scanner functions in the list, in priority order, stopping at
        # the first one that returns False.
        # Note that False is explicitly required: returning None will not stop
        # the scanner chain.
        for prio, scan in self.scanners:
            try:
                if scan(sample, md) is False:
                    break
            except Exception:
                log.warning("scanner function failed", exc_info=True)

    @classmethod
    def register(cls, scanner: Callable[[ScannedImage, arkimet.Metadata], None], priority=0):
        if scanner not in cls.scanners:
            cls.scanners.append((priority, scanner))
