# python 3.7+ from __future__ import annotations
import argparse
import logging
import os
import sys
from collections import defaultdict
from typing import Any, Dict, List

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
        return parser

    @classmethod
    def main(cls, args=None):
        parser = cls.make_parser()
        args = parser.parse_args(args=args)

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
