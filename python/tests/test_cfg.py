import unittest
import tempfile
import io
import arkimet as arki


class TestCfgSections(unittest.TestCase):
    def test_empty(self):
        sections = arki.cfg.Sections()
        self.assertEqual(len(sections), 0)
        with self.assertRaises(KeyError):
            sections["missing"]

    def test_obtain(self):
        sections = arki.cfg.Sections()
        self.assertIsNone(sections.section("name"))
        section = sections.obtain("name")
        self.assertIsInstance(section, arki.cfg.Section)
        self.assertEqual(sections.section("name"), section)
        section["key"] = "val"
        self.assertEqual(sections["name"]["key"], "val")

    def test_parse(self):
        body = """
[name]
key = val
"""
        with tempfile.NamedTemporaryFile("w+t") as fd:
            fd.write(body)
            fd.flush()
            sections = arki.cfg.Sections.parse(fd.name)
        self.assertEqual(sections["name"]["key"], "val")

        sections = arki.cfg.Sections.parse(io.StringIO(body))
        self.assertEqual(sections["name"]["key"], "val")

    def test_write(self):
        sections = arki.cfg.Sections()
        sections.obtain("name")["key"] = "val"
        with io.StringIO() as fd:
            sections.write(fd)
            res = fd.getvalue()
        self.assertEqual(res, "[name]\nkey = val\n")


class TestCfgSection(unittest.TestCase):
    def test_empty(self):
        section = arki.cfg.Section()
        self.assertEqual(len(section), 0)
        with self.assertRaises(KeyError):
            section["missing"]

    def test_assign(self):
        section = arki.cfg.Section()
        section["key1"] = "val1"
        section["key2"] = "val2"
        self.assertEqual(section["key1"], "val1")
        self.assertEqual(section["key2"], "val2")

    def test_parse(self):
        body = """
key = val
"""
        with tempfile.NamedTemporaryFile("w+t") as fd:
            fd.write(body)
            fd.flush()
            section = arki.cfg.Section.parse(fd.name)
        self.assertEqual(section["key"], "val")

        section = arki.cfg.Section.parse(io.StringIO(body))
        self.assertEqual(section["key"], "val")

    def test_write(self):
        section = arki.cfg.Section()
        section["key"] = "val"
        with io.StringIO() as fd:
            section.write(fd)
            res = fd.getvalue()
        self.assertEqual(res, "key = val\n")
