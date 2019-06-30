import unittest
import arkimet as arki


class TestCfgSections(unittest.TestCase):
    def test_empty(self):
        sections = arki.cfg.Sections()
        self.assertEqual(len(sections), 0)
        with self.assertRaises(KeyError):
            sections["missing"]


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
