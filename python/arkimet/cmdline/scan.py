# python 3.7+ from __future__ import annotations
import argparse
import logging
import posix
import sys

import arkimet
from arkimet.cmdline.base import AppConfigMixin, AppWithProcessor, Exit


class Scan(AppConfigMixin, AppWithProcessor):
    """
    Read one or more files or datasets and process their data
    or import them in a dataset.
    """
    log = logging.getLogger("arki-scan")

    @classmethod
    def make_parser(cls) -> argparse.ArgumentParser:
        parser = super().make_parser()

        parser.add_argument("source", nargs="*",
                            help="input files or datasets")

        # Inputs
        parser_in = parser.add_argument_group("in", "input options")
        parser_in.add_argument("--stdin", metavar="format",
                               help="read input from standard input in the given format")

        # arki-scan
        parser_in.add_argument("--files", metavar="file",
                               help="read the list of files to scan from the given file"
                                    " instead of the command line")

        parser_dis = parser.add_argument_group(
           "dispatch", "Options controlling dispatching data to datasets")

        parser_dis.add_argument("--moveok", metavar="directory",
                                help="move input files imported successfully to the given directory "
                                     "(destination directory must be on the same filesystem of source file)")
        parser_dis.add_argument("--moveko", metavar="directory",
                                help="move input files with problems to the given directory "
                                     "(destination directory must be on the same filesystem of source file)")
        parser_dis.add_argument("--movework", metavar="directory",
                                help="move input files here before opening them. This is useful to "
                                     "catch the cases where arki-scan crashes without having a "
                                     "chance to handle errors "
                                     "(destination directory must be on the same filesystem of source file)")

        parser_dis.add_argument("--copyok", metavar="directory",
                                help="copy the data from input files that was imported successfully"
                                     " to the given directory")
        parser_dis.add_argument("--copyko", metavar="directory",
                                help="copy the data from input files that had problems"
                                     " to the given directory")

        parser_dis.add_argument("--ignore-duplicates", action="store_true",
                                help="do not consider the run unsuccessful in case of duplicates")
        parser_dis.add_argument("--validate", metavar="checks",
                                help="run the given checks on the input data before dispatching"
                                     " (comma-separated list; use 'list' to get a list)")
        parser_dis.add_argument("--dispatch", metavar="conffile", action="append",
                                help="dispatch the data to the datasets described in the "
                                     "given configuration file (or a dataset path can also "
                                     "be given), then output the metadata of the data that "
                                     "has been dispatched (can be specified multiple times)")
        parser_dis.add_argument("--testdispatch", metavar="conffile", action="append",
                                help="simulate dispatching the files right after scanning,"
                                     " using the given configuration file or dataset directory"
                                     " (can be specified multiple times)")

        parser_dis.add_argument("--status", action="store_true",
                                help="print to standard error a line per every file with a summary"
                                     " of how it was handled")

        parser_dis.add_argument("--flush-threshold", metavar="size",
                                help="import a batch as soon as the data read so far exceeds"
                                     " this amount of megabytes (default: 128Mi; use 0 to load all"
                                     " in RAM no matter what)")

        return parser

    def build_config(self):
        self.sources = []
        if self.args.stdin is not None:
            if self.args.source or self.args.files:
                self.parser.error("you cannot specify input files or datasets when using --stdin")
        else:
            if self.args.files is not None:
                with open(self.args.files, "rt") as fd:
                    self.sources = [x.strip() for x in fd.readlines()]
                self.sources += self.args.source
            else:
                self.sources = self.args.source

            # From command line arguments, looking for data files or datasets
            for source in self.sources:
                if source == "-":
                    self.parser.error("use --stdin to read data from standard input")
                section = arkimet.dataset.read_config(source)
                self.add_config_section(section)

            if not self.session.has_datasets():
                self.parser.error("you need to specify at least one input file or dataset")

        if (self.args.dispatch or self.args.testdispatch) and self.args.stdin:
            self.parser.error("--stdin cannot be used together with --dispatch")

    def run(self):
        super().run()
        self.build_config()

        with self.outfile() as outfd:
            arki_scan = arkimet.cmdline.ArkiScan(self.session)
            arki_scan.set_processor(
                    query=arkimet.Matcher(),
                    outfile=outfd,
                    yaml=self.args.yaml,
                    json=self.args.json,
                    annotate=self.args.annotate,
                    inline=self.args.inline,
                    data=self.args.data,
                    summary=self.args.summary,
                    summary_short=self.args.summary_short,
                    summary_restrict=self.args.summary_restrict,
                    archive=self.args.archive,
                    postproc=self.args.postproc,
                    postproc_data=self.args.postproc_data,
                    sort=self.args.sort,
            )

            if self.args.dispatch or self.args.testdispatch:
                kw = dict(
                    copyok=self.args.copyok,
                    copyko=self.args.copyko,
                    validate=self.args.validate,
                    flush_threshold=(
                        self.parse_size(self.args.flush_threshold) if self.args.flush_threshold is not None else 0),
                )

                if self.args.dispatch:
                    dispatch_session = arkimet.dataset.Session()
                    for source in self.args.dispatch:
                        for name, cfg in arkimet.cfg.Sections.parse(source).items():
                            cfg["name"] = name
                            dispatch_session.add_dataset(cfg)
                    kw["dispatch"] = dispatch_session
                elif self.args.testdispatch:
                    dispatch_session = arkimet.dataset.Session()
                    for source in self.args.testdispatch:
                        for name, cfg in arkimet.cfg.Sections.parse(source).items():
                            cfg["name"] = name
                            dispatch_session.add_dataset(cfg)
                    kw["testdispatch"] = dispatch_session

                arki_scan.set_dispatcher(**kw)

                if self.args.stdin:
                    return arki_scan.dispatch_file(
                            sys.stdin, self.args.stdin,
                            ignore_duplicates=self.args.ignore_duplicates,
                            status=self.args.status)
                else:
                    return arki_scan.dispatch_sections(
                            moveok=self.args.moveok, moveko=self.args.moveko,
                            movework=self.args.movework,
                            ignore_duplicates=self.args.ignore_duplicates,
                            status=self.args.status)
            else:
                if self.args.stdin:
                    all_successful = arki_scan.scan_file(sys.stdin, self.args.stdin)
                else:
                    all_successful = arki_scan.scan_sections()

                if not all_successful:
                    raise Exit(posix.EX_DATAERR)
