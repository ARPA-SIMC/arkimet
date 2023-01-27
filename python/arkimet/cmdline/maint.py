# python 3.7+ from __future__ import annotations
import argparse
import logging
import sys

import arkimet
from arkimet.cmdline.base import ArkimetCommand, Exit, Fail


class Maint(ArkimetCommand):
    """
    Perform maintenance operations on arkimet datasets
    """
    log = logging.getLogger("arki-maint")

    @classmethod
    def make_parser(cls) -> argparse.ArgumentParser:
        parser = super().make_parser()
        subparsers = parser.add_subparsers(help="sub-command help", dest="command")
        return parser

    @classmethod
    def main(cls, args=None):
        parser = cls.make_parser()
        args = parser.parse_args(args=args)

        try:
            with args.command(args) as cmd:
                return cmd.run()
        except Exit as e:
            if not e.args:
                return
            else:
                return e.args[0]
        except Fail as e:
            print(e, file=sys.stderr)
            return 1
