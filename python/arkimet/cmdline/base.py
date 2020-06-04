import arkimet as arki
import argparse
import logging
from contextlib import contextmanager
import itertools
import sys
import re
import os
import posix


class Fail(Exception):
    pass


class Exit(Exception):
    pass


class PosixArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        self.print_help(sys.stderr)
        self.exit(posix.EX_USAGE, '%s: error: %s\n' % (self.prog, message))


class App:
    NAME = None

    def __init__(self):
        self.parser = PosixArgumentParser(description=self.get_description())
        self.parser.add_argument("--verbose", "-v", action="store_true",
                                 help="verbose output")
        self.parser.add_argument("--debug", action="store_true",
                                 help="debug output")
        self.parser.add_argument("--version", action="store_true",
                                 help="print the program version and exit")
        self.args = None

    def get_description(self):
        return self.__doc__

    def parse_args(self, args=None):
        self.args = self.parser.parse_args(args=args)

    def check_mutually_exclusive_options(self, *names):
        for first, second in itertools.combinations(names, 2):
            if getattr(self.args, first) and getattr(self.args, second):
                self.parser.error("--{} conflicts with --{}".format(first, second))

    def setup_logging(self):
        log_format = "%(levelname)s %(message)s"
        level = logging.WARN
        if self.args.debug:
            level = logging.DEBUG
        elif self.args.verbose:
            level = logging.INFO
        logging.basicConfig(level=level, stream=sys.stderr, format=log_format)

    def run(self):
        arki.set_verbosity(verbose=self.args.verbose, debug=self.args.debug)

        if self.args.version:
            import arkimet
            print(arkimet.get_version())
            raise Exit()

        self.setup_logging()

    @classmethod
    def main(cls, args=None):
        try:
            cmd = cls()
            cmd.parse_args(args)
            cmd.run()
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


class AppConfigMixin:
    def __init__(self):
        super().__init__()
        self.session = arki.dataset.Session()
        self.config = arki.cfg.Sections()

    def add_config_section(self, section, name=None):
        if name is None:
            name = section["name"]

        old = self.config.section(name)
        if old is not None:
            self.log.warning("ignoring dataset %s in %s, which has the same name as the dataset in %s",
                             name, section["path"], old["path"])
            return
        self.config[name] = section
        self.config[name]["name"] = name

    re_stringlist = re.compile(r"[\s,]+")

    def filter_restrict(self, restrict):
        """
        Filter self.config leaving only those sections whose restrict entry
        matches the given filter
        """
        to_remove = []
        allowed = frozenset(x for x in self.re_stringlist.split(restrict) if x)

        # An empty filter matches every config entry
        if not allowed:
            return

        for name, section in self.config.items():
            r = section.get("restrict", None)
            if r is None:
                to_remove.append(name)
                continue
            offered = frozenset(x for x in self.re_stringlist.split(r) if x)
            if not (allowed & offered):
                to_remove.append(name)

        for name in to_remove:
            del self.config[name]


class AppWithProcessor(App):
    def __init__(self):
        super().__init__()
        # Output options
        self.parser_out = self.parser.add_argument_group("out", "output options")
        self.parser_out.add_argument("--yaml", "--dump", action="store_true",
                                     help="dump the metadata as human-readable Yaml records")
        self.parser_out.add_argument("--json", action="store_true",
                                     help="dump the metadata in JSON format")
        self.parser_out.add_argument("--annotate", action="store_true",
                                     help="annotate the human-readable Yaml output with field descriptions")

        self.parser_out.add_argument("--summary", action="store_true",
                                     help="output only the summary of the data")
        self.parser_out.add_argument("--summary-short", action="store_true",
                                     help="output a list of all metadata values that exist in the summary of the data")
        self.parser_out.add_argument("--summary-restrict", metavar="types",
                                     help="summarise using only the given metadata types (comma-separated list)")
        self.parser_out.add_argument("--archive", metavar="format", nargs="?", const="tar",
                                     help="output an archive with the given format"
                                          " (default: tar, supported: tar, tar.gz, tar.xz, zip)")

        self.parser_out.add_argument("--output", "-o", metavar="file",
                                     help="write the output to the given file instead of standard output")
        self.parser_out.add_argument("--sort", metavar="period:order",
                                     help="sort order.  Period can be year, month, day, hour or minute."
                                          " Order can be a list of one or more metadata"
                                          " names (as seen in --yaml field names), with a '-'"
                                          " prefix to indicate reverse order.  For example,"
                                          " 'day:origin, -timerange, reftime' orders one day at a time"
                                          " by origin first, then by reverse timerange, then by reftime."
                                          " Default: do not sort")

        self.parser_out.add_argument("--inline", action="store_true",
                                     help="output the binary metadata together with the data (pipe through "
                                          " arki-dump or arki-grep to estract the data afterwards)")
        self.parser_out.add_argument("--data", action="store_true",
                                     help="output only the data")
        self.parser_out.add_argument("--postproc", "-p", metavar="command",
                                     help="output only the data, postprocessed with the given filter")
        self.parser_out.add_argument("--postproc-data", metavar="file", action="append",
                                     help="when querying a remote server with postprocessing, upload a file"
                                          " to be used by the postprocessor (can be given more than once)")

    @contextmanager
    def outfile(self):
        if not self.args.output or self.args.output == "-":
            yield sys.stdout
        else:
            with open(self.args.output, "wb") as fd:
                yield fd

    def run(self):
        super().run()

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
