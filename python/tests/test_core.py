import unittest
import arkimet as arki


class TestImport(unittest.TestCase):
    def test_import(self):
        import inspect
        self.assertTrue(inspect.ismodule(arki))
        self.assertTrue(inspect.isclass(arki.dataset.Reader))

    def test_matcher_alias_database(self):
        with arki.dataset.Session() as session:
            db = session.get_alias_database()
        self.assertTrue("origin" in db)

    def test_config(self):
        cfg = arki.config()
        self.assertIsInstance(cfg, dict)
