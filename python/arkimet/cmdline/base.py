import arkimet as arki
import argparse
import logging
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
        self.log = logging.getLogger(self.get_name())
        self.args = None

    def get_description(self):
        return self.__doc__

    def get_name(self):
        name = self.NAME
        if name is not None:
            return name
        return "arki-" + self.__class__.__name__[4:].lower()

    def parse_args(self, args=None):
        self.args = self.parser.parse_args(args=args)

    def check_mutually_exclusive_options(self, *names):
        for first, second in itertools.combinations(names, 2):
            if getattr(self.args, first) and getattr(self.args, second):
                self.parser.error("--{} conflicts with --{}".format(first, second))

    def run(self):
        if self.args.version:
            import arkimet
            print(arkimet.get_version())
            raise Exit()

        log_format = "%(levelname)s %(message)s"
        level = logging.WARN
        if self.args.debug:
            level = logging.DEBUG
        elif self.args.verbose:
            level = logging.INFO
        logging.basicConfig(level=level, stream=sys.stderr, format=log_format)

        import arkimet
        arkimet.set_verbosity(verbose=self.args.verbose, debug=self.args.debug)

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


class AppConfigMixin:
    def __init__(self):
        super().__init__()
        self.config = arki.cfg.Sections()

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
        self.parser_out.add_argument("--report", metavar="name",
                                     help="produce the given report with the command output")

        self.parser_out.add_argument("--summary", action="store_true",
                                     help="output only the summary of the data")
        self.parser_out.add_argument("--summary-short", action="store_true",
                                     help="output a list of all metadata values that exist in the summary of the data")
        self.parser_out.add_argument("--summary-restrict", metavar="types",
                                     help="summarise using only the given metadata types (comma-separated list)")
        self.parser_out.add_argument("--archive", metavar="format", nargs="?", const="tar",
                                     help="output an archive with the given format"
                                          " (default: tar, supported: tar, tar.gz, tar.xz, zip)")

        self.parser_out.add_argument("--outfile", "-o", metavar="file",
                                     help="write the output to the given file instead of standard output")
        self.parser_out.add_argument("--targetfile", metavar="pattern",
                                     help="append the output to file names computed from the data"
                                          " to be written. See /etc/arkimet/targetfile for details.")
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

    def run(self):
        super().run()

        self.check_mutually_exclusive_options(
                "inline", "summary", "summary_short", "data",
                "postproc", "archive", "json")

        self.check_mutually_exclusive_options(
                "inline", "yaml", "summary_short", "data",
                "postproc", "archive", "json")

        self.check_mutually_exclusive_options(
                "inline", "yaml", "report", "summary_short", "data",
                "postproc", "archive", "json")

        self.check_mutually_exclusive_options(
                "annotate", "inline", "report", "summary_short", "data",
                "postproc", "archive")

        self.check_mutually_exclusive_options(
                "sort", "report", "summary", "summary_short")

        if self.args.summary_restrict and not self.args.summary:
            self.parser.error("--summary-restrict only makes sense with --summary")

        if self.args.postproc and self.args.targetfile:
            self.parser.error("--postproc conflicts with --targetfile")

        if self.args.postproc_data and not self.args.postproc:
            self.parser.error("--postproc-data only makes sense with --postproc")

        if self.args.postproc_data:
            # Pass files for the postprocessor in the environment
            os.environ["ARKI_POSTPROC_FILES"] = ":".join(self.args.postproc_data)
        else:
            os.environ.pop("ARKI_POSTPROC_FILES", None)
