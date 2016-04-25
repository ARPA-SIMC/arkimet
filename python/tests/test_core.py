import unittest
import arkimet as arki

class TestImport(unittest.TestCase):
    def test_import(self):
        import inspect
        self.assertTrue(inspect.ismodule(arki))
        self.assertTrue(inspect.isclass(arki.DatasetReader))
