import unittest
import os
import arkimet as arki
import datetime
from pathlib import Path


class TestScanJPEG(unittest.TestCase):
    def read(self, pathname: Path, size: int, format="jpeg"):
        """
        Read all the metadata from a file
        """
        with arki.dataset.Session() as session:
            with session.dataset_reader(
                cfg={
                    "format": format,
                    "name": pathname.name,
                    "path": str(pathname),
                    "type": "file",
                }
            ) as ds:
                mds = ds.query_data()
        self.assertEqual(len(mds), 1)
        md = mds[0]

        source = md.to_python("source")
        self.assertEqual(
            source,
            {
                "type": "source",
                "style": "BLOB",
                "format": "jpeg",
                "basedir": os.getcwd(),
                "file": str(pathname),
                "offset": 0,
                "size": size,
            },
        )

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

    def test_autumn(self) -> None:
        """
        Scan a JPEG file with GPS coordinates
        """
        md = self.read(Path("inbound/jpeg/autumn.jpg"), 94701)
        self.assertNotIn("origin", md)
        self.assertNotIn("level", md)
        self.assertNotIn("timerange", md)
        self.assertNotIn("task", md)
        self.assertNotIn("quantity", md)
        self.assertNotIn("run", md)
        # TODO: scan product
        self.assertNotIn("product", md)
        self.assertEqual(
            md.to_python("area"),
            {
                "style": "GRIB",
                "type": "area",
                "value": {
                    "lat": 459497,
                    "lon": 110197,
                },
            },
        )
        self.assertEqual(md["reftime"], "2021-10-24T11:11:39Z")

    def test_test(self) -> None:
        """
        Scan a JPEG file with GPS coordinates
        """
        md = self.read(Path("inbound/jpeg/test.jpg"), 79127)
        self.assertNotIn("origin", md)
        self.assertNotIn("level", md)
        self.assertNotIn("timerange", md)
        self.assertNotIn("task", md)
        self.assertNotIn("quantity", md)
        self.assertNotIn("run", md)
        # TODO: scan product
        self.assertNotIn("product", md)
        self.assertEqual(
            md.to_python("area"),
            {
                "style": "GRIB",
                "type": "area",
                "value": {
                    "lat": 445008,
                    "lon": 113287,
                },
            },
        )
        self.assertEqual(md["reftime"], "2021-11-09T11:45:19Z")

    def test_recsh(self) -> None:
        basedir = Path("inbound/jpeg/camera/RECSH")
        for time in (
            datetime.time(8, 0),
            datetime.time(8, 30),
            datetime.time(9, 0),
            datetime.time(9, 30),
            datetime.time(10, 0),
        ):
            with self.subTest(time=time):
                hhmm = time.strftime("%H%M")
                md = self.read(basedir / f"20250820{hhmm}00_RECSH_1_20250820_{hhmm}.jpg", 36312)
                self.assertEqual(
                    md.to_python("area"), {"style": "GRIB", "type": "area", "value": {"lat": 439889, "lon": 126839}}
                )
                self.assertEqual(
                    md.to_python("reftime"),
                    {
                        "type": "reftime",
                        "style": "POSITION",
                        "time": datetime.datetime(2025, 8, 20, time.hour, time.minute),
                    },
                )
                self.assertEqual(
                    md.to_python("level"),
                    {
                        "type": "level",
                        "style": "GRIB2S",
                        "level_type": 1,
                        "scale": 1,
                        "value": None,
                    },
                )
                self.assertEqual(
                    md.to_python("timerange"),
                    {
                        "stat_len": 900,
                        "stat_type": 205,
                        "stat_unit": 13,
                        "step_len": 0,
                        "step_unit": 13,
                        "style": "Timedef",
                        "type": "timerange",
                    },
                )

    def test_rectm(self) -> None:
        basedir = Path("inbound/jpeg/camera/RECTM")
        for time in (
            datetime.time(8, 0),
            datetime.time(8, 30),
            datetime.time(9, 0),
            datetime.time(9, 30),
            datetime.time(10, 0),
        ):
            with self.subTest(time=time):
                hhmm = time.strftime("%H%M")
                md = self.read(basedir / f"20250820{hhmm}00_RECTM_1_20250820_{hhmm}.jpg", 36288)
                self.assertEqual(
                    md.to_python("area"), {"style": "GRIB", "type": "area", "value": {"lat": 439889, "lon": 126839}}
                )
                self.assertEqual(
                    md.to_python("reftime"),
                    {
                        "type": "reftime",
                        "style": "POSITION",
                        "time": datetime.datetime(2025, 8, 20, time.hour, time.minute),
                    },
                )
                self.assertEqual(
                    md.to_python("level"),
                    {
                        "type": "level",
                        "style": "GRIB2S",
                        "level_type": 1,
                        "scale": 1,
                        "value": None,
                    },
                )
                self.assertEqual(
                    md.to_python("timerange"),
                    {
                        "stat_len": 900,
                        "stat_type": 0,
                        "stat_unit": 13,
                        "step_len": 0,
                        "step_unit": 13,
                        "style": "Timedef",
                        "type": "timerange",
                    },
                )

    def test_snapshot(self) -> None:
        basedir = Path("inbound/jpeg/camera/SNAPSHOT")
        for time in (
            datetime.time(8, 0),
            datetime.time(8, 15),
            datetime.time(8, 30),
            datetime.time(8, 45),
            datetime.time(9, 0),
            datetime.time(9, 15),
            datetime.time(9, 30),
            datetime.time(9, 45),
            datetime.time(10, 0),
            datetime.time(10, 15),
        ):
            with self.subTest(time=time):
                hhmm = time.strftime("%H%M")
                md = self.read(basedir / f"20250820{hhmm}00_SNAPSHOT_1_20250820_{hhmm}.jpg", 34658)
                self.assertEqual(
                    md.to_python("area"), {"style": "GRIB", "type": "area", "value": {"lat": 439889, "lon": 126839}}
                )
                self.assertEqual(
                    md.to_python("reftime"),
                    {
                        "type": "reftime",
                        "style": "POSITION",
                        "time": datetime.datetime(2025, 8, 20, time.hour, time.minute),
                    },
                )
                self.assertEqual(
                    md.to_python("level"),
                    {
                        "type": "level",
                        "style": "GRIB2S",
                        "level_type": 1,
                        "scale": 1,
                        "value": None,
                    },
                )
                self.assertEqual(
                    md.to_python("timerange"),
                    {
                        "stat_len": 0,
                        "stat_type": 254,
                        "stat_unit": 13,
                        "step_len": 0,
                        "step_unit": 13,
                        "style": "Timedef",
                        "type": "timerange",
                    },
                )

    def test_t1stk(self) -> None:
        basedir = Path("inbound/jpeg/camera/T1STK")
        for time in (
            datetime.time(8, 0),
            datetime.time(8, 30),
            datetime.time(9, 0),
            datetime.time(9, 30),
            datetime.time(10, 0),
        ):
            with self.subTest(time=time):
                hhmm = time.strftime("%H%M")
                md = self.read(basedir / f"20250820{hhmm}00_T1STK_1_20250820_{hhmm}.jpg", 8529)
                self.assertEqual(
                    md.to_python("area"), {"style": "GRIB", "type": "area", "value": {"lat": 439889, "lon": 126839}}
                )
                self.assertEqual(
                    md.to_python("reftime"),
                    {
                        "type": "reftime",
                        "style": "POSITION",
                        "time": datetime.datetime(2025, 8, 20, time.hour, time.minute),
                    },
                )
                self.assertEqual(
                    md.to_python("level"),
                    {
                        "type": "level",
                        "style": "GRIB2S",
                        "level_type": 1,
                        "scale": 1,
                        "value": None,
                    },
                )
                self.assertEqual(
                    md.to_python("timerange"),
                    {
                        "stat_len": 900,
                        "stat_type": 205,
                        "stat_unit": 13,
                        "step_len": 0,
                        "step_unit": 13,
                        "style": "Timedef",
                        "type": "timerange",
                    },
                )

    def test_timex(self) -> None:
        basedir = Path("inbound/jpeg/camera/TIMEX")
        for time in (
            datetime.time(8, 0),
            datetime.time(8, 30),
            datetime.time(9, 0),
            datetime.time(9, 30),
            datetime.time(10, 0),
        ):
            with self.subTest(time=time):
                hhmm = time.strftime("%H%M")
                md = self.read(basedir / f"20250820{hhmm}00_TIMEX_1_20250820_{hhmm}.jpg", 28958)
                self.assertEqual(
                    md.to_python("area"), {"style": "GRIB", "type": "area", "value": {"lat": 439889, "lon": 126839}}
                )
                self.assertEqual(
                    md.to_python("reftime"),
                    {
                        "type": "reftime",
                        "style": "POSITION",
                        "time": datetime.datetime(2025, 8, 20, time.hour, time.minute),
                    },
                )
                self.assertEqual(
                    md.to_python("level"),
                    {
                        "type": "level",
                        "style": "GRIB2S",
                        "level_type": 1,
                        "scale": 1,
                        "value": None,
                    },
                )
                self.assertEqual(
                    md.to_python("timerange"),
                    {
                        "stat_len": 900,
                        "stat_type": 0,
                        "stat_unit": 13,
                        "step_len": 0,
                        "step_unit": 13,
                        "style": "Timedef",
                        "type": "timerange",
                    },
                )

    def test_tmxsh(self) -> None:
        basedir = Path("inbound/jpeg/camera/TMXSH")
        for time in (
            datetime.time(8, 0),
            datetime.time(8, 30),
            datetime.time(9, 0),
            datetime.time(9, 30),
            datetime.time(10, 0),
        ):
            with self.subTest(time=time):
                hhmm = time.strftime("%H%M")
                md = self.read(basedir / f"20250820{hhmm}00_TMXSH_1_20250820_{hhmm}.jpg", 29012)
                self.assertEqual(
                    md.to_python("area"), {"style": "GRIB", "type": "area", "value": {"lat": 439889, "lon": 126839}}
                )
                self.assertEqual(
                    md.to_python("reftime"),
                    {
                        "type": "reftime",
                        "style": "POSITION",
                        "time": datetime.datetime(2025, 8, 20, time.hour, time.minute),
                    },
                )
                self.assertEqual(
                    md.to_python("level"),
                    {
                        "type": "level",
                        "style": "GRIB2S",
                        "level_type": 1,
                        "scale": 1,
                        "value": None,
                    },
                )
                self.assertEqual(
                    md.to_python("timerange"),
                    {
                        "stat_len": 900,
                        "stat_type": 205,
                        "stat_unit": 13,
                        "step_len": 0,
                        "step_unit": 13,
                        "style": "Timedef",
                        "type": "timerange",
                    },
                )
