import unittest
import os
import io
import tempfile
import arkimet as arki
import datetime


class TestMetadata(unittest.TestCase):
    def read(self, pathname, format="grib"):
        """
        Read all the metadata from a file
        """
        ds = arki.dataset.Reader({
            "format": format,
            "name": os.path.basename(pathname),
            "path": pathname,
            "type": "file",
        })
        return ds.query_data()

    def test_subscript(self):
        md = self.read("inbound/test.grib1")[0]

        self.assertIn("reftime", md)
        self.assertEqual(md["reftime"], "2007-07-08T13:00:00Z")

        py_reftime = md.to_python("reftime")
        self.assertEqual(py_reftime,
                         {"type": "reftime", "style": "POSITION",  "time": datetime.datetime(2007, 7, 8, 13, 0, 0)})

        md["reftime"] = "2019-07-30T12:00:00Z"
        self.assertEqual(md["reftime"], "2019-07-30T12:00:00Z")

        md["reftime"] = py_reftime
        self.assertEqual(md["reftime"], "2007-07-08T13:00:00Z")

        self.assertIn("source", md)

        self.assertEqual(md["source"], "BLOB(grib,{}:0+7218)".format(os.path.abspath("inbound/test.grib1")))

        py_source = md.to_python("source")
        self.assertEqual(py_source, {
            "basedir": os.getcwd(),
            "file": "inbound/test.grib1",
            "format": "grib",
            "offset": 0,
            "size": 7218,
            "style": "BLOB",
            "type": "source",
        })
        md["source"] = py_source
        self.assertEqual(md["source"], "BLOB(grib,{}:0+7218)".format(os.path.abspath("inbound/test.grib1")))

    def test_to_string(self):
        md = self.read("inbound/test.grib1")[0]
        self.assertEqual(md.to_string("origin"), "GRIB1(200, 000, 101)")
        self.assertEqual(md.to_string("source"), "BLOB(grib,{}:0+7218)".format(os.path.abspath("inbound/test.grib1")))

    def test_to_python(self):
        mds = self.read("inbound/test.grib1")
        self.assertEqual(len(mds), 3)
        data = mds[0].to_python()
        source = next(i for i in data["items"] if i["type"] == "source")
        self.assertEquals(source["offset"], 0)
        self.assertEquals(source["file"], "inbound/test.grib1")
        data = mds[1].to_python()
        source = next(i for i in data["items"] if i["type"] == "source")
        self.assertEquals(source["offset"], 7218)
        self.assertEquals(source["file"], "inbound/test.grib1")

    def test_read_write_bundle(self):
        # with open("inbound/test.grib1.arkimet", "rb") as fd:
        #     orig_data = fd.read()

        res = []

        def store_md(md):
            nonlocal res
            res.append(md)

        # I/O on named file
        self.assertTrue(arki.Metadata.read_bundle("inbound/test.grib1.arkimet", dest=store_md))
        self.assertEqual(len(res), 3)
        self.assertEqual(res[0].to_python("source"), {
            'type': 'source', 'style': 'BLOB', 'format': 'grib',
            'basedir': os.path.abspath("inbound"),
            'file': 'test.grib1',
            'offset': 0, 'size': 7218,
            })

        orig_md = res

        with tempfile.TemporaryFile() as fd:
            arki.Metadata.write_bundle(res, fd.name)
            fd.seek(0)
            res = arki.Metadata.read_bundle(fd, basedir="inbound")
        self.maxDiff = None
        self.assertEqual(res, orig_md)

        # I/O on named file with basedir and pathname
        res = arki.Metadata.read_bundle("inbound/test.grib1.arkimet", basedir="basedir", pathname="pathname")
        self.assertEqual(len(res), 3)
        self.assertEqual(res[0].to_python("source"), {
            'basedir': os.path.join(os.getcwd(), "basedir"),
            'type': 'source', 'style': 'BLOB', 'format': 'grib',
            'file': 'test.grib1',
            'offset': 0, 'size': 7218,
        })

        with tempfile.TemporaryFile() as fd:
            arki.Metadata.write_bundle(res, fd.name)
            fd.seek(0)
            res = arki.Metadata.read_bundle(fd)
        self.assertEqual(res, orig_md)

        # I/O on real file descriptor
        with open("inbound/test.grib1.arkimet", "rb") as fd:
            res = arki.Metadata.read_bundle(fd)
        self.assertEqual(len(res), 3)
        self.assertEqual(res[0].to_python("source"), {
            'basedir': os.path.abspath("inbound"),
            'type': 'source', 'style': 'BLOB', 'format': 'grib',
            'file': 'test.grib1',
            'offset': 0, 'size': 7218,
        })

        with tempfile.TemporaryFile() as fd:
            arki.Metadata.write_bundle(res, fd)
            fd.seek(0)
            res = arki.Metadata.read_bundle(fd)
        self.assertEqual(res, orig_md)

        # I/O on real file descriptor with basedir and pathname
        with open("inbound/test.grib1.arkimet", "rb") as fd:
            res = arki.Metadata.read_bundle(fd.fileno(), basedir="basedir", pathname="pathname")
        self.assertEqual(len(res), 3)
        self.assertEqual(res[0].to_python("source"), {
            'basedir': os.path.join(os.getcwd(), "basedir"),
            'type': 'source', 'style': 'BLOB', 'format': 'grib',
            'file': 'test.grib1',
            'offset': 0, 'size': 7218,
        })

        with tempfile.TemporaryFile() as fd:
            arki.Metadata.write_bundle(res, fd)
            fd.seek(0)
            res = arki.Metadata.read_bundle(fd)
        self.assertEqual(res, orig_md)

        # I/O on BytesIO
        with open("inbound/test.grib1.arkimet", "rb") as fd:
            buf = fd.read()
        with io.BytesIO(buf) as fd:
            res = arki.Metadata.read_bundle(fd, basedir="basedir")
        self.assertEqual(len(res), 3)
        self.assertEqual(res[0].to_python("source"), {
            'basedir': os.path.join(os.getcwd(), "basedir"),
            'type': 'source', 'style': 'BLOB', 'format': 'grib',
            'file': 'test.grib1',
            'offset': 0, 'size': 7218,
        })

        with io.BytesIO() as fd:
            arki.Metadata.write_bundle(res, fd)
            res = arki.Metadata.read_bundle(fd.getvalue())
        self.assertEqual(res, orig_md)

    def test_write_yaml(self):
        self.maxDiff = None
        md = self.read("inbound/test.grib1")[0]
        with io.BytesIO() as out:
            md.write(out, format="yaml")
            self.assertEqual(out.getvalue()[:12], b"Source: BLOB")
            self.assertNotIn(b'# sfc Surface (of the Earth, which includes sea surface)', out.getvalue())

            # Read from bytes()
            md1 = arki.Metadata.read_yaml(out.getvalue())
            self.assertEqual(md.to_python(), md1.to_python())

            # Read from str()
            md1 = arki.Metadata.read_yaml(out.getvalue().decode())
            self.assertEqual(md.to_python(), md1.to_python())

            # Read from binary abstract FD
            out.seek(0)
            md1 = arki.Metadata.read_yaml(out)
            self.assertEqual(md.to_python(), md1.to_python())

            # Read from string abstract FD
            with io.StringIO(out.getvalue().decode()) as infd:
                md1 = arki.Metadata.read_yaml(infd)
                self.assertEqual(md.to_python(), md1.to_python())

        with io.BytesIO() as out:
            md.write(out, format="yaml", annotate=True)
            self.assertEqual(out.getvalue()[:11], b"SummaryItem")
            self.assertIn(b'# sfc Surface (of the Earth, which includes sea surface)', out.getvalue())

            out.seek(0)
            md1 = arki.Metadata.read_yaml(out)
            self.assertEqual(md.to_python(), md1.to_python())

    def test_write_json(self):
        md = self.read("inbound/test.grib1")[0]
        with io.BytesIO() as out:
            md.write(out, format="json")
            self.assertEqual(out.getvalue()[:20], b'{"i":[{"t":"source",')
            self.assertNotIn(b'"desc":"', out.getvalue())

            # Read from bytes()
            md1 = arki.Metadata.read_json(out.getvalue())
            self.assertEqual(md.to_python(), md1.to_python())

            # Read from str()
            md1 = arki.Metadata.read_json(out.getvalue().decode())
            self.assertEqual(md.to_python(), md1.to_python())

            # Read from binary abstract FD
            out.seek(0)
            md1 = arki.Metadata.read_json(out)
            self.assertEqual(md.to_python(), md1.to_python())

            # Read from string abstract FD
            with io.StringIO(out.getvalue().decode()) as infd:
                md1 = arki.Metadata.read_json(infd)
                self.assertEqual(md.to_python(), md1.to_python())

        with io.BytesIO() as out:
            md.write(out, format="json", annotate=True)
            self.assertEqual(out.getvalue()[:20], b'{"i":[{"t":"source",')
            self.assertIn(b'"desc":"', out.getvalue())

            out.seek(0)
            md1 = arki.Metadata.read_json(out)
            self.assertEqual(md.to_python(), md1.to_python())
