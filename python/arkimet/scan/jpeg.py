from typing import Callable, Any, Dict
# import functools
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
    """
    Wrapper for PIL.Image with shortcut methods to access known EXIF tags
    """

    def __init__(self, im: Image):
        self.im = im

    # TODO: from python 3.8 we can optimize with cached_property
    # @functools.cached_property
    # def exif(self) -> Dict[int, Any]:
    #     """
    #     dict with raw EXIF information
    #     """
    #     return self.im.getexif()

    # @functools.cached_property
    # def gpsinfo(self) -> Dict[int, Any]:
    #     """
    #     dict with raw EXIF GPSInfo information
    #     """
    #     gpsinfo = self.exif.get(gpsinfo_tag)
    #     return {} if gpsinfo is None else gpsinfo

    @property
    def exif(self) -> Dict[int, Any]:
        """
        dict with raw EXIF information
        """
        res = getattr(self, "_exif", None)
        if res is None:
            res = self.im.getexif()
            self._exif = res
        return res

    @property
    def gpsinfo(self) -> Dict[int, Any]:
        """
        dict with raw EXIF GPSInfo information
        """
        res = getattr(self, "_gpsinfo", None)
        if res is None:
            gpsinfo = self.exif.get(gpsinfo_tag)
            res = {} if gpsinfo is None else gpsinfo
            self._gpsinfo = res
        return res

    def get_image(self, tag_name: str) -> Any:
        """
        Return the value of a tag by its name in PIL.ExifTags.TAGS
        """
        tagid = image_tags.get(tag_name)
        if tagid is None:
            raise KeyError(f"Exif tag {tag_name!r} is not available in PIL.ExifTags.TAGS")
        return self.exif[tagid]

    def get_gps(self, tag_name: str) -> Any:
        """
        Return the value of a GPSInfo tag by its name in PIL.ExifTags.GPSTAGs
        """
        tagid = gps_tags.get(tag_name)
        if tagid is None:
            raise KeyError(f"Exif tag {tag_name!r} is not available in PIL.ExifTags.GPSTAGS")
        return self.gpsinfo.get(tagid)

    def dump(self, file=None):
        """
        Dump all available keys, by name, to the given file descriptor
        (default: sys.stderr)
        """
        if file is None:
            file = sys.stderr

        lines = []

        for tagid, val in self.exif.items():
            if tagid in ExifTags.TAGS:
                lines.append(f"image:{ExifTags.TAGS[tagid]}: {val!r}")
            else:
                lines.append(f"image:unknown {tagid:x}: {val!r}")

        for tagid, val in self.gpsinfo.items():
            if tagid in ExifTags.GPSTAGS:
                lines.append(f"gps:{ExifTags.GPSTAGS[tagid]}: {val!r}")
            else:
                lines.append(f"gps:unknown {tagid:x}: {val!r}")

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
