# python 3.7+ from __future__ import annotations
import arkimet as arki
import argparse
import logging
from contextlib import contextmanager, ExitStack
import itertools
import sys
import re
import os

from . import cmdline

Fail = cmdline.Fail


class Exit(BaseException):
    pass


class PosixArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        self.print_help(sys.stderr)
        self.exit(os.EX_USAGE, '%s: error: %s\n' % (self.prog, message))


class ArkimetCommand(cmdline.Command):
    argumentparser_class = PosixArgumentParser

    def __init__(self, parser: argparse.ArgumentParser, args: argparse.Namespace):
        if args.version:
            import arkimet
            print(arkimet.get_version())
            raise Exit()
        super().__init__(parser, args)

    def setup_logging(self):
        arki.set_verbosity(verbose=self.args.verbose, debug=self.args.debug)

        log_format = "%(levelname)s %(message)s"
        level = logging.WARN
        if self.args.debug:
            level = logging.DEBUG
        elif self.args.verbose:
            level = logging.INFO
        logging.basicConfig(level=level, stream=sys.stderr, format=log_format)

    @classmethod
    def make_parser(cls) -> argparse.ArgumentParser:
        parser = super().make_parser()
        parser.add_argument("--version", action="store_true",
                            help="print the program version and exit")
        return parser


class App(ArkimetCommand):
    NAME = None

    def __init__(self, parser: argparse.ArgumentParser, args: argparse.Namespace):
        super().__init__(parser, args)
        self.stack = ExitStack()

    def check_mutually_exclusive_options(self, *names):
        for first, second in itertools.combinations(names, 2):
            if getattr(self.args, first) and getattr(self.args, second):
                self.parser.error("--{} conflicts with --{}".format(first, second))

    def run(self):
        pass

    def shutdown(self):
        pass

    def __enter__(self):
        self.stack.__enter__()
        return self

    def __exit__(self, *args):
        self.shutdown()
        return self.stack.__exit__(*args)

    @classmethod
    def main(cls, args=None):
        parser = cls.make_parser()
        args = parser.parse_args(args=args)

        try:
            with cls(parser, args) as cmd:
                return cmd.run()
        except Exit as e:
            if not e.args:
                return
            else:
                return e.args[0]
        except Fail as e:
            print(e, file=sys.stderr)
            return 1

    re_size = re.compile(r"^(\d+)(\w*)$")

    @classmethod
    def parse_size(cls, val: str):
        """
        Parse a string in the form <number><suffix> returning its value in bytes
        """
        mo = cls.re_size.match(val)
        if not mo:
            raise ValueError("invalid size: {}".format(repr(val)))

        base_val = int(mo.group(1))
        suffix = mo.group(2)

        if not suffix:
            return base_val

        if len(suffix) > 2:
            raise ValueError("invalid suffix {} in {}".format(repr(suffix), repr(val)))

        lsuffix = suffix.lower()

        if len(suffix) == 1:
            mul = 1000
        elif lsuffix[1] == 'b':
            if lsuffix[0] in ("b", "c"):
                raise ValueError("invalid suffix {} in {}".format(repr(suffix), repr(val)))
            mul = 1000
        elif lsuffix[1] == "i":
            mul = 1024
        else:
            raise ValueError("invalid suffix {} in {}".format(repr(suffix), repr(val)))

        if lsuffix[0] in ("b", "c"):
            return base_val
        elif lsuffix[0] == "k":
            return base_val * mul
        elif lsuffix[0] == "m":
            return base_val * (mul ** 2)
        elif lsuffix[0] == "g":
            return base_val * (mul ** 3)
        elif lsuffix[0] == "t":
            return base_val * (mul ** 4)
        elif lsuffix[0] == "p":
            return base_val * (mul ** 5)
        elif lsuffix[0] == "e":
            return base_val * (mul ** 6)
        elif lsuffix[0] == "z":
            return base_val * (mul ** 7)
        elif lsuffix[0] == "y":
            return base_val * (mul ** 8)
        else:
            raise ValueError("invalid suffix {} in {}".format(repr(suffix), repr(val)))


class RestrictSectionFilter:
    """
    Filter config sections based on 'restrict' values
    """
    re_stringlist = re.compile(r"[\s,]+")

    def __init__(self, restrict_pattern):
        self.allowed = frozenset(x for x in self.re_stringlist.split(restrict_pattern) if x)

    def is_allowed(self, cfg):
        if not self.allowed:
            return True

        r = cfg.get("restrict", None)
        if r is None:
            return False

        offered = frozenset(x for x in self.re_stringlist.split(r) if x)
        if not (self.allowed & offered):
            return False

        return True


class SystemDatasetSectionFilter:
    """
    Discard "error" and "duplicates" config sections
    """
    def is_allowed(self, cfg):
        type = cfg.get("type")
        name = cfg.get("name")

        return not (type == "error" or type == "duplicates" or
                    (type == "remote" and (name == "error" or name == "duplicates")))


class AppConfigMixin:
    def __init__(self, parser: argparse.ArgumentParser, args: argparse.Namespace):
        super().__init__(parser, args)
        self.session = self.stack.enter_context(arki.dataset.Session())

        self.config_filters = []
        self.config_filter_discarded = 0

        restrict = getattr(self.args, "restrict", None)
        if restrict is not None:
            self.config_filters.append(RestrictSectionFilter(restrict))

        if getattr(self.args, "ignore_system_datasets", False):
            self.config_filters.append(SystemDatasetSectionFilter())

    def add_config_section(self, section, name=None):
        # Ignore sections any of the current filters discards
        for filter in self.config_filters:
            if not filter.is_allowed(section):
                self.config_filter_discarded += 1
                return

        if name is None:
            name = section["name"]
        section["name"] = name
        self.session.add_dataset(section)


class AppWithProcessor(App):
    @classmethod
    def make_parser(cls) -> argparse.ArgumentParser:
        parser = super().make_parser()

        # Output options
        parser_out = parser.add_argument_group("out", "output options")
        parser_out.add_argument("--yaml", "--dump", action="store_true",
                                help="dump the metadata as human-readable Yaml records")
        parser_out.add_argument("--json", action="store_true",
                                help="dump the metadata in JSON format")
        parser_out.add_argument("--annotate", action="store_true",
                                help="annotate the human-readable Yaml output with field descriptions")

        parser_out.add_argument("--summary", action="store_true",
                                help="output only the summary of the data")
        parser_out.add_argument("--summary-short", action="store_true",
                                help="output a list of all metadata values that exist in the summary of the data")
        parser_out.add_argument("--summary-restrict", metavar="types",
                                help="summarise using only the given metadata types (comma-separated list)")
        parser_out.add_argument("--archive", metavar="format", nargs="?", const="tar",
                                help="output an archive with the given format"
                                     " (default: tar, supported: tar, tar.gz, tar.xz, zip)")

        parser_out.add_argument("--output", "-o", metavar="file",
                                help="write the output to the given file instead of standard output")
        parser_out.add_argument("--sort", metavar="period:order",
                                help="sort order.  Period can be year, month, day, hour or minute."
                                     " Order can be a list of one or more metadata"
                                     " names (as seen in --yaml field names), with a '-'"
                                     " prefix to indicate reverse order.  For example,"
                                     " 'day:origin, -timerange, reftime' orders one day at a time"
                                     " by origin first, then by reverse timerange, then by reftime."
                                     " Default: do not sort")

        parser_out.add_argument("--inline", action="store_true",
                                help="output the binary metadata together with the data (pipe through "
                                     " arki-dump to extract the data afterwards)")
        parser_out.add_argument("--data", action="store_true",
                                help="output only the data")
        parser_out.add_argument("--postproc", "-p", metavar="command",
                                help="output only the data, postprocessed with the given filter")
        parser_out.add_argument("--postproc-data", metavar="file", action="append",
                                help="when querying a remote server with postprocessing, upload a file"
                                     " to be used by the postprocessor (can be given more than once)")

        return parser

    def __init__(self, parser: argparse.ArgumentParser, args: argparse.Namespace):
        super().__init__(parser, args)

        self.check_mutually_exclusive_options(
                "inline", "summary", "summary_short", "data",
                "postproc", "archive")

        self.check_mutually_exclusive_options(
                "inline", "data", "postproc", "archive",
                "yaml", "json")

        self.check_mutually_exclusive_options(
                "inline", "data", "postproc", "archive",
                "annotate")

        self.check_mutually_exclusive_options(
                "sort", "summary", "summary_short")

        if self.args.summary_restrict and not self.args.summary:
            self.parser.error("--summary-restrict only makes sense with --summary")

        if self.args.postproc_data and not self.args.postproc:
            self.parser.error("--postproc-data only makes sense with --postproc")

        if self.args.postproc_data:
            # Pass files for the postprocessor in the environment
            os.environ["ARKI_POSTPROC_FILES"] = ":".join(self.args.postproc_data)
        else:
            os.environ.pop("ARKI_POSTPROC_FILES", None)

    @contextmanager
    def outfile(self):
        if not self.args.output or self.args.output == "-":
            yield sys.stdout
        else:
            with open(self.args.output, "wb") as fd:
                yield fd
