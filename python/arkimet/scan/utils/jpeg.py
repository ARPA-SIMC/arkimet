from typing import Any, Dict

# import functools
import sys

from PIL import Image, ExifTags

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
            # Introduced in https://pillow.readthedocs.io/en/stable/releasenotes/8.2.0.html
            gpsinfo = self.exif.get_ifd(gpsinfo_tag)
            if gpsinfo is None:
                # Fallback with pillow pre-8.2
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
        return self.exif.get(tagid)

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
