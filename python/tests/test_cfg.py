import unittest
import arkimet as arki


class TestCfgSections(unittest.TestCase):
    def test_sections(self):
        sections = arki.cfg.Sections()
        self.assertEqual(len(sections), 0)
