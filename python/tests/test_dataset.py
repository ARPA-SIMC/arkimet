import unittest
import tempfile
import arkimet as arki

class TestDatasetReader(unittest.TestCase):
    def test_create(self):
        ds = arki.DatasetReader({
            "format": "grib",
            "name": "test.grib1",
            "path": "inbound/test.grib1",
            "type": "file",
        })
        self.assertEqual(str(ds), "DatasetReader(file, test.grib1)");
        self.assertEqual(repr(ds), "DatasetReader(file, test.grib1)");

    def test_query_data(self):
        ds = arki.DatasetReader({
            "format": "grib",
            "name": "test.grib1",
            "path": "inbound/test.grib1",
            "type": "file",
            "postprocess": "countbytes",
        })

        count = 0
        def count_results(md):
            nonlocal count
            count += 1

        # No arguments
        ds.query_data(on_metadata=count_results)
        self.assertEquals(count, 3)

        # Matcher
        count = 0
        ds.query_data(matcher="reftime:=2007-07-08", on_metadata=count_results)
        self.assertEquals(count, 1)

        # Output
        with tempfile.TemporaryFile() as fd:
            def stream_results(md):
                md.write(fd)
            ds.query_data(on_metadata=stream_results)
            fd.seek(0)
            queried = fd.read()
        self.assertEquals(len(queried), 612)

        self.fail("sort still untested")

        self.fail("no way yet to test with_data")

    def test_query_summary(self):
        ds = arki.DatasetReader({
            "format": "grib",
            "name": "test.grib1",
            "path": "inbound/test.grib1",
            "type": "file",
            "postprocess": "countbytes",
        })

        # No arguments
        res = ds.query_summary()
        self.assertIsInstance(res, arki.Summary)

        # Matcher
        res = ds.query_summary(matcher="reftime:=2007-07-08")
        self.assertIsInstance(res, arki.Summary)

        # Add to existing summary
        s = arki.Summary()
        res = ds.query_summary(summary=s)
        self.assertIs(res, s)

        self.fail("no way yet to test summary contents")

    def test_query_bytes(self):
        ds = arki.DatasetReader({
            "format": "grib",
            "name": "test.grib1",
            "path": "inbound/test.grib1",
            "type": "file",
            "postprocess": "countbytes",
        })

        with open("inbound/test.grib1", "rb") as fd:
            orig = fd.read()

        # No arguments
        with tempfile.TemporaryFile() as fd:
            ds.query_bytes(fd)
            fd.seek(0)
            queried = fd.read()
        self.assertEqual(orig, queried)

        # matcher
        with tempfile.TemporaryFile() as fd:
            ds.query_bytes(fd, matcher="reftime:>=1900")
            fd.seek(0)
            queried = fd.read()
        self.assertEqual(orig, queried)

        # data_start_hook
        triggered = False;
        def data_start_hook():
            nonlocal triggered
            triggered = True
        with tempfile.TemporaryFile() as fd:
            ds.query_bytes(fd, data_start_hook=data_start_hook)
            fd.seek(0)
            queried = fd.read()
        self.assertEqual(orig, queried)
        self.assertTrue(triggered)

        # postprocess
        with tempfile.TemporaryFile() as fd:
            ds.query_bytes(fd, postprocess="countbytes")
            fd.seek(0)
            queried = fd.read()
        # This is bigger than 44412 because postprocessors are also sent
        # metadata, so that arki-xargs can work.
        self.assertEqual(queried, b"44961\n")

        # metadata_report
        with tempfile.TemporaryFile() as fd:
            ds.query_bytes(fd, metadata_report="count")
            fd.seek(0)
            queried = fd.read()
        self.assertEqual(queried, b"3\n")

        # summary_report
        with tempfile.TemporaryFile() as fd:
            ds.query_bytes(fd, summary_report="count")
            fd.seek(0)
            queried = fd.read()
        self.assertEqual(queried, b"3\n")
