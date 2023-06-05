# python 3.7+ from __future__ import annotations
import argparse
import logging

import arkimet
from arkimet.cmdline.base import App, Exit

log = logging.getLogger("arki-bufr-prepare")


class BufrPrepare(App):
    """
    Read BUFR messages, and encode each subsection in a separate message.
    """
    NAME = "arki-bufr-prepare"

    @classmethod
    def make_parser(cls) -> argparse.ArgumentParser:
        parser = super().make_parser()
        parser.add_argument("-o", "--output", metavar="file",
                            help="write the output to the given file instead of standard output")
        parser.add_argument("--fail", metavar="file",
                            help="do not ignore BUFR data that could not be decoded,"
                                 " but write it to the given file")
        parser.add_argument("--usn", metavar="number", type=lambda x: int(x) if x is not None else None,
                            help="overwrite the update sequence number of every BUFR message with this value")
        parser.add_argument("files", nargs="*", action="store",
                            help="BUFR files to process")
        return parser

    def run(self):
        super().run()

        cmd = arkimet.cmdline.ArkiBUFRPrepare()
        res = cmd.run(
            inputs=self.args.files,
            usn=self.args.usn,
            outfile=self.args.output,
            failfile=self.args.fail,
        )
        if res:
            raise Exit(res)
