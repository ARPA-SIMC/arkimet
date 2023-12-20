# python 3.7+ from __future__ import annotations
import argparse
import logging
import os
import sys
from collections import defaultdict
from typing import Any, Dict, List, Tuple

import arkimet
from arkimet.cmdline.base import ArkimetCommand, Exit, Fail


class Unarchive(ArkimetCommand):
    """
    Move archived segments back online
    """
    log = logging.getLogger("arki-maint")

    @classmethod
    def make_subparser(cls, subparsers: "argparse._SubParsersAction[Any]") -> argparse.ArgumentParser:
        parser = super().make_subparser(subparsers)
        parser.add_argument("segments", metavar="segment", nargs="+", action="store",
                            help="pathname of segment to unarchive")
        return parser

    def run(self):
        super().run()
        by_dataset: Dict[str, List[str]] = defaultdict(list)
        ARCPATH = os.sep + os.path.join(".archive", "last") + os.sep
        for path in self.args.segments:
            path = os.path.abspath(path)
            if ARCPATH not in path:
                self.log.warning("%s: ignoring file not inside .archive/last/", path)
                continue

            dataset, segment = path.split("/.archive/last/")
            by_dataset[dataset].append(segment)

        for dataset, segments in by_dataset.items():
            with arkimet.dataset.Session() as session:
                section = arkimet.dataset.read_config(dataset)
                session.add_dataset(section)
                arki_check = arkimet.cmdline.ArkiCheck(session)
                for segment in segments:
                    arki_check.unarchive(pathname=segment)


class ScanTest(ArkimetCommand):
    """
    Run a query on a dataset, rescan the resulting data, and show metadata changes
    """
    @classmethod
    def make_subparser(cls, subparsers: "argparse._SubParsersAction[Any]") -> argparse.ArgumentParser:
        parser = super().make_subparser(subparsers)
        parser.add_argument("query", metavar="query", action="store",
                            help="query to run")
        parser.add_argument("dataset", metavar="path", nargs="+", action="store",
                            help="dataset(s) to query")
        return parser

    TypesDict = Dict[str, List[Dict[str, Any]]]

    def index(self, md: arkimet.Metadata) -> Tuple[Dict[str, Any], TypesDict]:
        source = None
        res = defaultdict(list)
        for item in md.to_python()["items"]:
            if item["type"] == "source":
                source = item
            else:
                res[item["type"]].append(item)

        return source, res

    def compare(self, orig: TypesDict, new: arkimet.Metadata) -> Tuple[TypesDict, TypesDict]:
        added = defaultdict(list)
        for item in new.to_python()["items"]:
            if item["type"] == "source":
                continue

            try:
                orig[item["type"]].remove(item)
            except ValueError:
                added[item["type"]].append(item)

        # Remove empty lists from orig
        # FIXME: things can be implemented so that this step is unnecessary,
        #        but at the moment this code tries to focus on clarity rather
        #        than speed
        for name, vals in list(orig.items()):
            if not vals:
                del orig[name]

        return added, orig

    def run(self):
        super().run()
        ok = 0
        failed = 0
        with arkimet.dataset.Session() as session:
            query = session.matcher(self.args.query)
            for path in self.args.dataset:
                cfg = arkimet.dataset.read_config(path)
                with session.dataset_reader(cfg=cfg) as ds:
                    for orig in ds.query_data(query, with_data=True):
                        source, orig_data = self.index(orig)
                        scanner = arkimet.scan.Scanner.get_scanner(source["format"])
                        new = scanner.scan_data(orig.data)

                        added, removed = self.compare(orig_data, new)

                        if added or removed:
                            print("Mismatch in", source)
                            for name, values in added.items():
                                for value in values:
                                    print("  added:", name, value)
                            for name, values in removed.items():
                                for value in values:
                                    print("  removed:", name, value)
                            failed += 1
                        else:
                            ok += 1
        print(f"{ok} elements matched, {failed} elements had changes")


class Maint(ArkimetCommand):
    """
    Perform maintenance operations on arkimet datasets
    """
    log = logging.getLogger("arki-maint")

    @classmethod
    def make_parser(cls) -> argparse.ArgumentParser:
        parser = super().make_parser()
        subparsers = parser.add_subparsers(help="sub-command help", dest="command")
        Unarchive.make_subparser(subparsers)
        ScanTest.make_subparser(subparsers)
        return parser

    @classmethod
    def main(cls, args=None):
        parser = cls.make_parser()
        args = parser.parse_args(args=args)

        if args.command is None:
            parser.print_help()
            return

        try:
            with args.command(parser, args) as cmd:
                return cmd.run()
        except Exit as e:
            if not e.args:
                return
            else:
                return e.args[0]
        except Fail as e:
            print(e, file=sys.stderr)
            return 1
