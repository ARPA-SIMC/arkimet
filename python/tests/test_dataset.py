import unittest
import tempfile
import arkimet as arki
import os
import io
import shutil
from arkimet.test import daemon


class Progress:
    def __init__(self):
        self.start_called = 0
        self.update_called = 0
        self.done_called = 0
        self.total_count = 0
        self.total_bytes = 0

    def start(self, expected_count=0, expected_bytes=0):
        self.start_called += 1

    def update(self, count, bytes):
        self.update_called += 1

    def done(self, total_count, total_bytes):
        self.done_called += 1
        self.total_count = total_count
        self.total_bytes = total_bytes


class TestReadConfig(unittest.TestCase):
    def test_file(self):
        pathname = os.path.abspath("inbound/test.grib1")
        section = arki.dataset.read_config(pathname)
        contents = list(section.items())
        self.assertEqual(contents, [
            ('format', 'grib'),
            ('name', pathname),
            ('path', pathname),
            ('type', 'file'),
        ])

    def test_http(self):
        with daemon(os.path.join(os.environ["TOP_SRCDIR"], "arki/dataset/http-redirect-daemon")) as url:
            sections = arki.dataset.http.load_cfg_sections(url)
            self.assertEqual(sections.keys(), ('error', 'test200', 'test80'))


class TestDatasetReader(unittest.TestCase):
    def test_create(self):
        ds = arki.dataset.Reader({
            "format": "grib",
            "name": "test.grib1",
            "path": "inbound/test.grib1",
            "type": "file",
        })
        self.assertEqual(str(ds), "dataset.Reader(file, test.grib1)")
        self.assertEqual(repr(ds), "dataset.Reader(file, test.grib1)")

    def test_query_data(self):
        ds = arki.dataset.Reader({
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
        self.assertEquals(len(queried), 588)

        def query_reftimes(matcher=None, sort=None):
            res = []

            def on_metadata(md):
                info = md.to_python()["items"]
                for i in info:
                    if i["type"] != "reftime":
                        continue
                    res.append([i["time"].month, i["time"].day])
                    break
            ds.query_data(matcher=matcher, sort=sort, on_metadata=on_metadata)
            return res

        res = query_reftimes()
        self.assertEquals(res, [[7, 8], [7, 7], [10, 9]])
        res = query_reftimes(sort="reftime")
        self.assertEquals(res, [[7, 7], [7, 8], [10, 9]])
        res = query_reftimes(sort="-reftime")
        self.assertEquals(res, [[10, 9], [7, 8], [7, 7]])

        # self.fail("no way yet to test with_data")

    def test_query_summary(self):
        ds = arki.dataset.Reader({
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

        # Output
        summary = ds.query_summary()
        with tempfile.TemporaryFile() as fd:
            summary.write(fd)
            fd.seek(0)
            queried = fd.read()
        self.assertEqual(queried[:2], b"SU")

    def test_query_bytes(self):
        ds = arki.dataset.Reader({
            "format": "grib",
            "name": "inbound/test.grib1",
            "path": "inbound/test.grib1",
            "type": "file",
            "postprocess": "countbytes",
        })

        with open("inbound/test.grib1", "rb") as fd:
            orig = fd.read()

        # No arguments
        with tempfile.TemporaryFile() as fd:
            ds.query_bytes(file=fd)
            fd.seek(0)
            queried = fd.read()
        self.assertEqual(orig, queried)

        with io.BytesIO() as fd:
            ds.query_bytes(file=fd)
            queried = fd.getvalue()
        self.assertEqual(orig, queried)

        # matcher
        with tempfile.TemporaryFile() as fd:
            ds.query_bytes(file=fd, matcher="reftime:>=1900")
            fd.seek(0)
            queried = fd.read()
        self.assertEqual(orig, queried)

        with io.BytesIO() as fd:
            ds.query_bytes(file=fd, matcher="reftime:>=1900")
            queried = fd.getvalue()
        self.assertEqual(orig, queried)

        # data_start_hook
        triggered = False

        def data_start_hook():
            nonlocal triggered
            triggered = True
        with tempfile.TemporaryFile() as fd:
            ds.query_bytes(file=fd, data_start_hook=data_start_hook)
            fd.seek(0)
            queried = fd.read()
        self.assertEqual(orig, queried)
        self.assertTrue(triggered)

        # postprocess
        with tempfile.TemporaryFile() as fd:
            ds.query_bytes(file=fd, postprocess="countbytes")
            fd.seek(0)
            queried = fd.read()
        # This is bigger than 44412 because postprocessors are also sent
        # metadata, so that arki-xargs can work.
        self.assertEqual(queried, b"44937\n")

        with io.BytesIO() as fd:
            ds.query_bytes(file=fd, postprocess="countbytes")
            queried = fd.getvalue()
        # This is bigger than 44412 because postprocessors are also sent
        # metadata, so that arki-xargs can work.
        self.assertEqual(queried, b"44937\n")

        queried = ds.query_bytes(postprocess="countbytes")
        # This is bigger than 44412 because postprocessors are also sent
        # metadata, so that arki-xargs can work.
        self.assertEqual(queried, b"44937\n")

    def test_query_data_qmacro(self):
        ds = arki.make_qmacro_dataset(
            {},
            """
[test200]
format = grib
name = test.grib1
path = inbound/test.grib1
type = file
""",
            "expa 2007-07-08",
            "ds:test200. d:@. t:1300. s:GRIB1/0/0h/0h. l:GRIB1/1. v:GRIB1/200/140/229.\n",
        )

        count = 0

        def count_results(md):
            nonlocal count
            count += 1

        # No arguments
        ds.query_data(on_metadata=count_results)
        self.assertEquals(count, 1)

    def test_query_data_merged(self):
        ds = arki.make_merged_dataset(
            """
[test200]
format = grib
name = test.grib1
path = inbound/test.grib1
type = file
""",
        )

        count = 0

        def count_results(md):
            nonlocal count
            count += 1

        # No arguments
        ds.query_data(on_metadata=count_results)
        self.assertEquals(count, 3)

    def test_query_data_memoryusage(self):
        ds = arki.dataset.Reader({
            "type": "testlarge",
        })
        count = 0

        def count_results(md):
            nonlocal count
            count += 1

        # No arguments
        ds.query_data(on_metadata=count_results)
        self.assertEquals(count, 24841)

    def test_progress(self):
        ds = arki.dataset.Reader({
            "format": "grib",
            "name": "test.grib1",
            "path": "inbound/test.grib1",
            "type": "file",
        })

        count = 0

        def count_results(md):
            nonlocal count
            count += 1

        progress = Progress()
        ds.query_data(on_metadata=count_results, progress=progress)
        self.assertEquals(count, 3)
        self.assertEqual(progress.total_count, 3)
        self.assertGreaterEqual(progress.total_bytes, 90)
        self.assertEqual(progress.start_called, 1)
        # Initial and final calls at least, but intermediate calls are
        # throttled at no more than one each 200ms
        self.assertGreaterEqual(progress.update_called, 2)
        self.assertEqual(progress.done_called, 1)

        progress = Progress()
        res = ds.query_bytes(progress=progress)
        self.assertEquals(len(res), 44412)

        self.assertEqual(progress.total_count, 3)
        self.assertGreaterEqual(progress.total_bytes, 90)
        self.assertEqual(progress.start_called, 1)
        # Initial and final calls at least, but intermediate calls are
        # throttled at no more than one each 200ms
        self.assertGreaterEqual(progress.update_called, 2)
        self.assertEqual(progress.done_called, 1)

    def test_progess_qmacro(self):
        ds = arki.make_qmacro_dataset(
            {},
            """
[test200]
format = grib
name = test.grib1
path = inbound/test.grib1
type = file
""",
            "expa 2007-07-08",
            "ds:test200. d:@. t:1300. s:GRIB1/0/0h/0h. l:GRIB1/1. v:GRIB1/200/140/229.\n",
        )

        count = 0

        def count_results(md):
            nonlocal count
            count += 1

        progress = Progress()

        # No arguments
        ds.query_data(on_metadata=count_results, progress=progress)
        self.assertEquals(count, 1)
        self.assertEqual(progress.total_count, 1)
        self.assertEqual(progress.total_bytes, 7218)
        self.assertEqual(progress.start_called, 1)
        self.assertEqual(progress.update_called, 1)
        self.assertEqual(progress.done_called, 1)

        progress = Progress()
        res = ds.query_bytes(progress=progress)
        self.assertEquals(len(res), 7218)
        self.assertEqual(progress.total_count, 1)
        self.assertEqual(progress.total_bytes, 7218)
        self.assertEqual(progress.start_called, 1)
        self.assertEqual(progress.update_called, 1)
        self.assertEqual(progress.done_called, 1)

    def test_progress_merged(self):
        ds = arki.make_merged_dataset(
            """
[test1]
format = grib
name = test.grib1
path = inbound/test.grib1
type = file

[test2]
format = bufr
name = test.bufr
path = inbound/test.bufr
type = file
""",
        )

        count = 0

        def count_results(md):
            nonlocal count
            count += 1

        progress = Progress()

        # No arguments
        ds.query_data(on_metadata=count_results, progress=progress)
        self.assertEquals(count, 6)
        self.assertEqual(progress.total_count, 6)
        self.assertEqual(progress.total_bytes, 45046)
        self.assertEqual(progress.start_called, 1)
        # Initial and final calls at least, but intermediate calls are
        # throttled at no more than one each 200ms
        self.assertGreaterEqual(progress.update_called, 2)
        self.assertEqual(progress.done_called, 1)

        progress = Progress()
        res = ds.query_bytes(progress=progress)
        self.assertEquals(len(res), 45046)
        self.assertEqual(progress.total_count, 6)
        self.assertEqual(progress.total_bytes, 45046)
        self.assertEqual(progress.start_called, 1)
        # Initial and final calls at least, but intermediate calls are
        # throttled at no more than one each 200ms
        self.assertGreaterEqual(progress.update_called, 2)
        self.assertEqual(progress.done_called, 1)


class TestDatasetWriter(unittest.TestCase):
    def test_import(self):
        try:
            shutil.rmtree("testds")
        except FileNotFoundError:
            pass
        os.mkdir("testds")

        dest = arki.dataset.Writer({
            "format": "grib",
            "name": "testds",
            "path": "testds",
            "type": "iseg",
            "step": "daily",
        })

        source = arki.dataset.Reader({
            "format": "grib",
            "name": "test.grib1",
            "path": "inbound/test.grib1",
            "type": "file",
        })

        def do_import(md):
            dest.acquire(md)

        source.query_data(on_metadata=do_import)
        dest.flush()

        dest = arki.dataset.Reader({
            "format": "grib",
            "name": "testds",
            "path": "testds",
            "type": "iseg",
            "step": "daily",
        })

        count = 0

        def count_results(md):
            nonlocal count
            count += 1

        dest.query_data(on_metadata=count_results)
        self.assertEqual(count, 3)
