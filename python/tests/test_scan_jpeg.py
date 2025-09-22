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
                        "l1": 1,
                        "l2": None,
                        "scale1": 1,
                        "scale2": 1,
                        "style": "GRIB2D",
                        "type": "level",
                        "value1": None,
                        "value2": None,
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
        md = self.read(basedir / "20250820080000_RECTM_1_20250820_0800.jpg", 36288)
        md = self.read(basedir / "20250820083000_RECTM_1_20250820_0830.jpg", 36288)
        md = self.read(basedir / "20250820090000_RECTM_1_20250820_0900.jpg", 36288)
        md = self.read(basedir / "20250820093000_RECTM_1_20250820_0930.jpg", 36288)
        md = self.read(basedir / "20250820100000_RECTM_1_20250820_1000.jpg", 36288)

    def test_snapshot(self) -> None:
        basedir = Path("inbound/jpeg/camera/SNAPSHOT")
        md = self.read(basedir / "20250820080000_SNAPSHOT_1_20250820_0800.jpg", 34658)
        md = self.read(basedir / "20250820081500_SNAPSHOT_1_20250820_0815.jpg", 34658)
        md = self.read(basedir / "20250820083000_SNAPSHOT_1_20250820_0830.jpg", 34658)
        md = self.read(basedir / "20250820084500_SNAPSHOT_1_20250820_0845.jpg", 34658)
        md = self.read(basedir / "20250820090000_SNAPSHOT_1_20250820_0900.jpg", 34658)
        md = self.read(basedir / "20250820091500_SNAPSHOT_1_20250820_0915.jpg", 34658)
        md = self.read(basedir / "20250820093000_SNAPSHOT_1_20250820_0930.jpg", 34658)
        md = self.read(basedir / "20250820094500_SNAPSHOT_1_20250820_0945.jpg", 34658)
        md = self.read(basedir / "20250820100000_SNAPSHOT_1_20250820_1000.jpg", 34658)
        md = self.read(basedir / "20250820101500_SNAPSHOT_1_20250820_1015.jpg", 34658)

    def test_t1stk(self) -> None:
        basedir = Path("inbound/jpeg/camera/T1STK")
        md = self.read(basedir / "20250820080000_T1STK_1_20250820_0800.jpg", 8529)
        md = self.read(basedir / "20250820083000_T1STK_1_20250820_0830.jpg", 8529)
        md = self.read(basedir / "20250820090000_T1STK_1_20250820_0900.jpg", 8529)
        md = self.read(basedir / "20250820093000_T1STK_1_20250820_0930.jpg", 8529)
        md = self.read(basedir / "20250820100000_T1STK_1_20250820_1000.jpg", 8529)

    def test_timex(self) -> None:
        basedir = Path("inbound/jpeg/camera/TIMEX")
        md = self.read(basedir / "20250820080000_TIMEX_1_20250820_0800.jpg", 28958)
        md = self.read(basedir / "20250820083000_TIMEX_1_20250820_0830.jpg", 28958)
        md = self.read(basedir / "20250820090000_TIMEX_1_20250820_0900.jpg", 28958)
        md = self.read(basedir / "20250820093000_TIMEX_1_20250820_0930.jpg", 28958)
        md = self.read(basedir / "20250820100000_TIMEX_1_20250820_1000.jpg", 28958)

    def test_tmxsh(self) -> None:
        basedir = Path("inbound/jpeg/camera/TMXSH")
        md = self.read(basedir / "20250820080000_TMXSH_1_20250820_0800.jpg", 29012)
        md = self.read(basedir / "20250820083000_TMXSH_1_20250820_0830.jpg", 29012)
        md = self.read(basedir / "20250820090000_TMXSH_1_20250820_0900.jpg", 29012)
        md = self.read(basedir / "20250820093000_TMXSH_1_20250820_0930.jpg", 29012)
        md = self.read(basedir / "20250820100000_TMXSH_1_20250820_1000.jpg", 29012)
