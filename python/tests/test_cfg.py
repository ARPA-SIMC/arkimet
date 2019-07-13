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

        self.assertFalse("name" in sections)

    def test_obtain(self):
        sections = arki.cfg.Sections()
        self.assertIsNone(sections.section("name"))
        section = sections.obtain("name")
        self.assertIsInstance(section, arki.cfg.Section)
        self.assertEqual(sections.section("name"), section)
        section["key"] = "val"
        self.assertEqual(sections["name"]["key"], "val")
        self.assertEqual(sections.get("name"), section)
        self.assertEqual(sections.get("name", 42), section)
        self.assertIsNone(sections.get("nonexisting"))
        self.assertEqual(sections.get("nonexisting", 42), 42)

        self.assertTrue("name" in sections)

    def test_iter(self):
        sections = arki.cfg.Sections()
        sections.obtain("sec2")
        sections.obtain("sec1")
        self.assertEqual([x for x in sections], ["sec1", "sec2"])

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

    def test_items(self):
        sections = arki.cfg.Sections()
        sections.obtain("sec1")["key1"] = "val1"
        sections.obtain("sec2")["key2"] = "val2"
        sections.obtain("sec3")["key3"] = "val3"
        a = list(sections.items())
        self.assertEqual(a[0][0], "sec1")
        self.assertEqual(a[1][0], "sec2")
        self.assertEqual(a[2][0], "sec3")
        self.assertEqual(a[0][1]["key1"], "val1")
        self.assertEqual(a[1][1]["key2"], "val2")
        self.assertEqual(a[2][1]["key3"], "val3")


class TestCfgSection(unittest.TestCase):
    def test_empty(self):
        section = arki.cfg.Section()
        self.assertEqual(len(section), 0)
        with self.assertRaises(KeyError):
            section["missing"]

        self.assertFalse("key" in section)

    def test_init(self):
        section = arki.cfg.Section({"key": "val"})
        self.assertEqual(section["key"], "val")

        section = arki.cfg.Section(key="val")
        self.assertEqual(section["key"], "val")

    def test_iter(self):
        section = arki.cfg.Section()
        section["key2"] = "val2"
        section["key1"] = "val1"
        self.assertEqual([x for x in section], ["key1", "key2"])

    def test_assign(self):
        section = arki.cfg.Section()
        section["key1"] = "val1"
        section["key2"] = "val2"
        self.assertEqual(section["key1"], "val1")
        self.assertEqual(section["key2"], "val2")

        self.assertEqual(section.get("key1"), "val1")
        self.assertEqual(section.get("key1", 42), "val1")
        self.assertIsNone(section.get("nonexisting"))
        self.assertEqual(section.get("nonexisting", 42), 42)

        self.assertTrue("key1" in section)

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
