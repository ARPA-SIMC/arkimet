# python 3.7+ from __future__ import annotations
import argparse
import json
import logging
import pathlib
import sys
from contextlib import contextmanager

import arkimet
from arkimet.cmdline.base import App, Exit

log = logging.getLogger("arki-dump")


class ConfigEncoder(json.JSONEncoder):
    """Serialize JSON including pathlib.Path."""

    def default(self, obj):
        if isinstance(obj, pathlib.Path):
            return obj.as_posix()
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


class Dump(App):
    """
    Read data from the given input file (or stdin), and dump the in human readable format on stdout
    """

    @classmethod
    def make_parser(cls) -> argparse.ArgumentParser:
        parser = super().make_parser()
        parser.add_argument(
            "-o", "--output", metavar="file", help="write the output to the given file instead of standard output"
        )
        parser.add_argument("input", action="store", nargs="?", help="file to read (or stdin if omitted)")

        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            "--query",
            action="store_true",
            help="print a query (specified on the command line) with the aliases expanded",
        )
        group.add_argument(
            "--config",
            action="store_true",
            help="print the arkimet configuration used to access the given file or dataset or URL",
        )
        group.add_argument(
            "--aliases",
            action="store_true",
            help="dump the alias database (to dump the aliases of a remote server,"
            " put the server URL on the command line)",
        )
        group.add_argument("--info", action="store_true", help="dump configuration information")
        group.add_argument("--bbox", action="store_true", help="dump the bounding box")
        group.add_argument(
            "--from-yaml-data", action="store_true", help="read a Yaml data dump and write binary metadata"
        )
        group.add_argument(
            "--from-yaml-summary", action="store_true", help="read a Yaml summary dump and write a binary summary"
        )
        group.add_argument(
            "--annotate", action="store_true", help="annotate the human-readable Yaml output with field descriptions"
        )
        group.add_argument("--doc-metadata", action="store_true", help="Print documentation of metadata types")

        return parser

    @contextmanager
    def input(self, mode):
        if not self.args.input or self.args.input == "-":
            yield sys.stdin
        else:
            with open(self.args.input, mode) as fd:
                yield fd

    @contextmanager
    def output(self, mode):
        if not self.args.output or self.args.output == "-":
            yield sys.stdout
        else:
            with open(self.args.output, mode) as fd:
                yield fd

    def run(self):
        super().run()

        if self.args.query:
            if not self.args.input:
                self.parser.error("--query needs a query on the command line")
            with arkimet.dataset.Session() as session:
                matcher = session.matcher(self.args.input)
                print(matcher.expanded)
            raise Exit()

        if self.args.aliases:
            if self.args.input:
                sections = arkimet.dataset.http.get_alias_database(self.args.input)
            else:
                with arkimet.dataset.Session() as session:
                    sections = session.get_alias_database()

            if self.args.output:
                with open(self.args.output, "wt") as fd:
                    sections.write(fd)
            else:
                sections.write(sys.stdout)
            raise Exit()

        if self.args.config:
            self.parser.error("please use arki-mergeconf instead of arki-dump --config")

        if self.args.info:
            cfg = arkimet.config()
            print(json.dumps(cfg, indent=1, cls=ConfigEncoder))
            raise Exit()

        if self.args.bbox:
            dump = arkimet.cmdline.ArkiDump()
            with self.input("rb") as fd:
                bbox = dump.bbox(fd)
            with self.output("wt") as fd:
                print(bbox, file=fd)
            raise Exit()

        if self.args.from_yaml_data:
            dump = arkimet.cmdline.ArkiDump()
            with self.input("rb") as fdin:
                with self.output("wb") as fdout:
                    raise Exit(dump.reverse_data(fdin, fdout))

        if self.args.from_yaml_summary:
            dump = arkimet.cmdline.ArkiDump()
            with self.input("rb") as fdin:
                with self.output("wb") as fdout:
                    raise Exit(dump.reverse_summary(fdin, fdout))

        if self.args.doc_metadata:
            arkimet.Metadata.print_type_documentation(sys.stdout)
            raise Exit()

        dump = arkimet.cmdline.ArkiDump()
        with self.input("rb") as fdin:
            with self.output("wb") as fdout:
                raise Exit(dump.dump_yaml(fdin, fdout, self.args.annotate))
