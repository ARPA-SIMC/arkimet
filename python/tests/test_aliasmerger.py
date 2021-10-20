from typing import Dict
import unittest
import arkimet as arki
from arkimet.dataset.http.aliases import AliasMerger


class MockLogger:
    def __init__(self):
        self.warnings = []
        self.errors = []

    def warning(self, fmt, *args):
        self.warnings.append(fmt % args)

    def error(self, fmt, *args):
        self.errors.append(fmt % args)


class TestAliasMerger(unittest.TestCase):
    def make_server_cfg(self, data: Dict[str, Dict[str, Dict[str, str]]]):
        """
        Give a dict in the form {servername: {section: {aliasname: matcher}}},
        convert it to a dict in the form {servername: arki.cfg.Sections}
        """
        res = {}
        for srv, sections in data.items():
            res[srv] = s = arki.cfg.Sections()
            for secname, section in sections.items():
                s[secname] = arki.cfg.Section(section)
        return res

    def assertMerges(self, data: Dict[str, Dict[str, Dict[str, str]]]):
        """
        Create an AliasMerger with the given data
        """
        merger = AliasMerger(MockLogger())
        by_server = self.make_server_cfg(data)
        merger.merge_servers(by_server)
        return merger

    def assertMergeFails(self, data: Dict[str, Dict[str, Dict[str, str]]]):
        """
        Try to create an AliasMerger with the given data, and expect an
        exception to be raised for a conflict
        """
        merger = AliasMerger(MockLogger())
        by_server = self.make_server_cfg(data)
        with self.assertRaises(RuntimeError):
            merger.merge_servers(by_server)
        return merger

    def test_all_same(self):
        merger = self.assertMerges({
            "server1": {"origin": {"test": "GRIB1 or GRIB2"}},
            "server2": {"origin": {"test": "GRIB1 or GRIB2"}},
        })

        self.assertEqual(merger.log.warnings, [])
        self.assertEqual(merger.log.errors, [])

        self.assertCountEqual(merger.aliases.keys(), ["origin"])
        self.assertCountEqual(merger.aliases["origin"].keys(), ["test"])
        self.assertEqual(merger.aliases["origin"]["test"], "GRIB1 or GRIB2")

    def test_section_missing(self):
        merger = self.assertMerges({
            "server1": {
                "origin": {"test": "GRIB1 or GRIB2"},
            },
            "server2": {
                "origin": {"test": "GRIB1 or GRIB2"},
                "product": {"test": "GRIB1 or GRIB2"},
            },
        })

        self.assertEqual(merger.log.warnings, [
            "server server1: no aliases found in section [product]."
            " Skipping this section of aliases in the merged database",
        ])
        self.assertEqual(merger.log.errors, [])

        self.assertCountEqual(merger.aliases.keys(), ["origin"])
        self.assertCountEqual(merger.aliases["origin"].keys(), ["test"])
        self.assertEqual(merger.aliases["origin"]["test"], "GRIB1 or GRIB2")

    def test_alias_missing(self):
        merger = self.assertMerges({
            "server1": {
                "origin": {
                    "test": "GRIB1 or GRIB2",
                    "test1": "BUFR or GRIB2",
                },
            },
            "server2": {
                "origin": {"test": "GRIB1 or GRIB2"},
            },
        })

        self.assertEqual(merger.log.warnings, [
            "Server server1: [origin] test1 = 'BUFR or GRIB2'",
            "Server server2: [origin] test1 = None",
            "[origin] test1: skipped as not available in all servers",
        ])
        self.assertEqual(merger.log.errors, [])

        self.assertCountEqual(merger.aliases.keys(), ["origin"])
        self.assertCountEqual(merger.aliases["origin"].keys(), ["test"])
        self.assertEqual(merger.aliases["origin"]["test"], "GRIB1 or GRIB2")

    def test_alias_conflict(self):
        merger = self.assertMergeFails({
            "server1": {"origin": {"test": "GRIB1 or GRIB2"}},
            "server2": {"origin": {"test": "BUFR or GRIB2"}},
        })

        self.assertEqual(merger.log.warnings, [
            "Server server1: [origin] test = 'GRIB1 or GRIB2'",
            "Server server2: [origin] test = 'BUFR or GRIB2'",
        ])
        self.assertEqual(merger.log.errors, [
            "[origin] test: not all servers have the same value",
        ])

        self.assertCountEqual(merger.aliases.keys(), [])
