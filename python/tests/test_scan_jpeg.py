import unittest
import os
import arkimet as arki
import datetime


class TestScanJPEG(unittest.TestCase):
    def read(self, pathname, size, format="jpeg"):
        """
        Read all the metadata from a file
        """
        ds = arki.dataset.Reader({
            "format": format,
            "name": os.path.basename(pathname),
            "path": pathname,
            "type": "file",
        })
        mds = ds.query_data()
        self.assertEqual(len(mds), 1)
        md = mds[0]

        source = md.to_python("source")
        self.assertEqual(source, {
            "type": "source",
            "style": "BLOB",
            "format": "jpeg",
            "basedir": os.getcwd(),
            "file": pathname,
            "offset": 0,
            "size": size,
        })

        data = md.data
        self.assertEqual(len(data), size)
        self.assertEqual(data[:2], b"\xff\xd8")
        self.assertEqual(data[-2:], b"\xff\xd9")

        notes = md.get_notes()
        self.assertEqual(len(notes), 1)
        self.assertEqual(notes[0]["type"], "note")
        self.assertEqual(notes[0]["value"], "Scanned from {}".format(os.path.basename(pathname)))
        self.assertIsInstance(notes[0]["time"], datetime.datetime)
        return md

    def test_autumn(self):
        """
        Scan a JPEG file with GPS coordinates
        """
        md = self.read("inbound/jpeg/autumn.jpg", 94701)
        self.assertNotIn("origin", md)
        self.assertNotIn("level", md)
        self.assertNotIn("timerange", md)
        self.assertNotIn("task", md)
        self.assertNotIn("quantity", md)
        self.assertNotIn("run", md)
        # TODO: scan product
        self.assertNotIn("product", md)
        # TODO: scan area
        # GPS Altitude                    : 956 m Above Sea Level
        # GPS Date/Time                   : 2021:10:24 11:11:39Z
        # GPS Latitude                    : 45 deg 56' 59.00" N
        # GPS Longitude                   : 11 deg 1' 11.00" E
        # GPS Latitude Ref                : North
        # GPS Altitude Ref                : Above Sea Level
        # GPS Longitude Ref               : East
        # GPS Time Stamp                  : 11:11:39
        # GPS Date Stamp                  : 2021:10:24
        # GPS Img Direction               : 277.37
        # GPS Img Direction Ref           : Magnetic North
        # GPS Position                    : 45 deg 56' 59.00" N, 11 deg 1' 11.00" E
        self.assertEqual(md.to_python("area"), {
            'style': 'GRIB',
            'type': 'area',
            'value': {
                "type": 0,
                "latfirst": 459497,
                "latlast": 459497,
                "lonfirst": 110197,
                "lonlast": 110197,
            },
        })
        # File Modification Date/Time     : 2021:10:24 13:11:43+02:00
        # File Access Date/Time           : 2021:11:24 11:30:52+01:00
        # File Inode Change Date/Time     : 2021:11:24 11:30:46+01:00
        # Date/Time Original              : 2021:10:24 13:11:40
        # Create Date                     : 2021:10:24 13:11:40
        # Create Date                     : 2021:10:24 13:11:40.0367
        # Modify Date                     : 2021:10:24 13:11:40
        # Date/Time Original              : 2021:10:24 13:11:40.0367
        # Modify Date                     : 2021:10:24 13:11:40.0367
        self.assertEqual(md["reftime"], "2021-10-24T13:11:40Z")

        # ExifTool Version Number         : 12.16
        # File Name                       : autumn.jpg
        # Directory                       : samples
        # File Size                       : 6.3 MiB
        # File Permissions                : rw-r--r--
        # File Type                       : JPEG
        # File Type Extension             : jpg
        # MIME Type                       : image/jpeg
        # Exif Byte Order                 : Big-endian (Motorola, MM)
        # Compression                     : JPEG (old-style)
        # Camera Model Name               : SM-A520F
        # Make                            : samsung
        # Software                        : SAMSUNG
        # Aperture Value                  : 1.9
        # Scene Type                      : Unknown (?)
        # Exposure Compensation           : 0
        # Exposure Program                : Program AE
        # Color Space                     : sRGB
        # Max Aperture Value              : 1.9
        # Brightness Value                : 6.45
        # Sub Sec Time Original           : 0367
        # White Balance                   : Auto
        # Exposure Mode                   : Auto
        # Exposure Time                   : 1/435
        # Flash                           : No Flash
        # Sub Sec Time                    : 0367
        # F Number                        : 1.9
        # User Comment                    : Yaw:277.36716,Pitch:7.980882089647679,Roll:0.5228175682852907
        # Saturation                      : Normal
        # ISO                             : 100
        # Components Configuration        : Err (63), Err (63), Err (63), -
        # Focal Length In 35mm Format     : 27 mm
        # Sub Sec Time Digitized          : 0367
        # Contrast                        : Normal
        # Sharpness                       : Normal
        # Digital Zoom Ratio              : 0
        # Shutter Speed Value             : 1/434
        # Focal Length                    : 3.6 mm
        # Metering Mode                   : Spot
        # Scene Capture Type              : Portrait
        # Light Source                    : Unknown
        # Orientation                     : Unknown (0)
        # JFIF Version                    : 1.01
        # Resolution Unit                 : None
        # X Resolution                    : 1
        # Y Resolution                    : 1
        # Profile CMM Type                :
        # Profile Version                 : 2.1.0
        # Profile Class                   : Display Device Profile
        # Color Space Data                : RGB
        # Profile Connection Space        : XYZ
        # Profile Date Time               : 0000:00:00 00:00:00
        # Profile File Signature          : acsp
        # Primary Platform                : Unknown ()
        # CMM Flags                       : Not Embedded, Independent
        # Device Manufacturer             :
        # Device Model                    :
        # Device Attributes               : Reflective, Glossy, Positive, Color
        # Rendering Intent                : Media-Relative Colorimetric
        # Connection Space Illuminant     : 0.9642 1 0.82491
        # Profile Creator                 :
        # Profile ID                      : 0
        # Profile Description             : sRGB
        # Red Matrix Column               : 0.43607 0.22249 0.01392
        # Green Matrix Column             : 0.38515 0.71687 0.09708
        # Blue Matrix Column              : 0.14307 0.06061 0.7141
        # Red Tone Reproduction Curve     : (Binary data 40 bytes, use -b option to extract)
        # Green Tone Reproduction Curve   : (Binary data 40 bytes, use -b option to extract)
        # Blue Tone Reproduction Curve    : (Binary data 40 bytes, use -b option to extract)
        # Media White Point               : 0.9642 1 0.82491
        # Profile Copyright               : Google Inc. 2016
        # Image Width                     : 4608
        # Image Height                    : 3456
        # Encoding Process                : Baseline DCT, Huffman coding
        # Bits Per Sample                 : 8
        # Color Components                : 3
        # Y Cb Cr Sub Sampling            : YCbCr4:2:0 (2 2)
        # Aperture                        : 1.9
        # Image Size                      : 4608x3456
        # Megapixels                      : 15.9
        # Scale Factor To 35 mm Equivalent: 7.5
        # Shutter Speed                   : 1/435
        # Circle Of Confusion             : 0.004 mm
        # Field Of View                   : 67.4 deg
        # Focal Length                    : 3.6 mm (35 mm equivalent: 27.0 mm)
        # Hyperfocal Distance             : 1.70 m
        # Light Value                     : 10.6
