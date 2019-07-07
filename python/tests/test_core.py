import unittest
import arkimet as arki


class TestImport(unittest.TestCase):
    def test_import(self):
        import inspect
        self.assertTrue(inspect.ismodule(arki))
        self.assertTrue(inspect.isclass(arki.dataset.Reader))

    def test_matcher_alias_database(self):
        import configparser
        db = arki.matcher_alias_database()
        cfg = configparser.ConfigParser()
        cfg.read_string(db)
        self.assertTrue(cfg.has_section("origin"))
