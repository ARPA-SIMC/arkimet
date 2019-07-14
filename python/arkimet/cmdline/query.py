import arkimet as arki
from arkimet.cmdline.base import App
import itertools
import re
import os
import logging

log = logging.getLogger("arki-query")

re_stringlist = re.compile(r"[\s,]+")


class Query(App):
    """
    Query the datasets in the given config file for data matching the given
    expression, and output the matching metadata.
    """

    def __init__(self):
        super().__init__()
        self.config = None

        self.parser_out = self.parser.add_argument_group("out", "output options")

        # Inputs
        self.parser.add_argument("--stdin", metavar="format",
                                 help="read input from standard input in the given format")
        self.parser.add_argument("query", nargs="?",
                                 help="arkimet query to run")
        self.parser.add_argument("source", nargs="*",
                                 help="input files or datasets")

        # Output options
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
        self.parser_out.add_argument("--archive", metavar="format", nargs="?",
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

        # arki-query
        self.parser.add_argument("--config", "-C", metavar="file", action="append",
                                 help="read configuration about input sources from the given file"
                                      " (can be given more than once)")
        self.parser.add_argument("--restrict", metavar="names",
                                 help="restrict operations to only those datasets that allow one"
                                      " of the given (comma separated) names")

        self.parser.add_argument("--merged", action="store_true",
                                 help="if multiple datasets are given, merge their data and output it in"
                                      " reference time order.  Note: sorting does not work when using"
                                      " --postprocess, --data or --report")

        self.parser.add_argument("--file", "-f", metavar="file",
                                 help="read the query expression from the given file")

        self.parser.add_argument("--qmacro", metavar="name",
                                 help="run the given query macro instead of a plain query")

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

    def _add_config(self, section, name=None):
        if name is None:
            name = section["name"]

        old = self.config.section(name)
        if old is not None:
            log.warning("ignoring dataset %s in %s, which has the same name as the dataset in %s",
                        name, section["path"], old["path"])
        self.config[name] = section
        self.config[name]["name"] = name

    def build_config(self):
        self.config = arki.cfg.Sections()
        self.query = None
        self.sources = []
        if self.args.file is not None:
            with open(self.args.file, "rt") as fd:
                self.query = fd.read()
            if self.args.query is not None:
                self.sources = [self.args.query] = self.args.source
        else:
            self.query = self.args.query
            self.sources = self.args.source

        if self.args.stdin is not None:
            if self.sources:
                self.parser.error("you cannot specify input files or datasets when using --stdin")
            if self.args.config:
                self.parser.error("--stdin cannot be used together with --config")
            if self.args.restrict:
                self.parser.error("--stdin cannot be used together with --restr")
            if self.args.qmacro:
                self.parser.error("--stdin cannot be used together with --qmacro")
        else:
            # From -C options, looking for config files or datasets
            if self.args.config:
                for pathname in self.args.config:
                    cfg = arki.dataset.read_configs(pathname)
                    for name, section in cfg.items():
                        self._add_config(section, name)

            # From command line arguments, looking for data files or datasets
            for source in self.sources:
                section = arki.dataset.read_config(source)
                self._add_config(section)

            if not self.config:
                self.parser.error("you need to specify at least one input file or dataset")

            # Remove unallowed entries
            if self.args.restrict:
                to_remove = []
                allowed = frozenset(x for x in re_stringlist.split(self.args.restrict) if x)
                for name, section in self.config.items():
                    r = section.get("restrict", None)
                    if r is None:
                        to_remove.append(name)
                        continue
                    offered = frozenset(x for x in re_stringlist.split(r) if x)
                    if not (allowed & offered):
                        to_remove.append(name)

                for name in to_remove:
                    del self.config[name]

            if not self.config:
                self.parser.error("no accessible datasets found for the given --restrict value")

            # Some things cannot be done when querying multiple datasets at the same time
            if len(self.config) > 1 and not self.args.qmacro:
                if self.args.postprocess:
                    self.parser.error(
                            "postprocessing is not possible when querying more than one dataset at the same time")
                if self.args.report:
                    self.parser.error("reports are not possible when querying more than one dataset at the same time")

    def check_mutually_exclusive_options(self, *names):
        for first, second in itertools.combinations(names, 2):
            if getattr(self.args, first) and getattr(self.args, second):
                self.parser.error("--{} conflicts with --{}".format(first, second))

    def run(self):
        super().run()
        self.build_config()

        self.check_mutually_exclusive_options(
                "inline", "yaml", "summary", "summary_short", "data",
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

        # Parse the matcher query
        if self.args.qmacro:
            strquery = ""
            qmacro_query = self.query
        else:
            if self.query is None:
                self.parser.error("you need to specify a filter expression or use --file")
            strquery = self.query
            qmacro_query = None

        if strquery:
            query = arki.Matcher(arki.dataset.http.expand_remote_query(strquery))
        else:
            query = arki.Matcher()

        arki_query = arki.ArkiQuery()
        arki_query.set_inputs(self.config)
        arki_query.set_processor(
                query=query,
                outfile=self.args.outfile or "-",
                yaml=self.args.yaml,
                json=self.args.json,
                annotate=self.args.annotate,
                inline=self.args.inline,
                data=self.args.data,
                summary=self.args.summary,
                summary_short=self.args.summary_short,
                report=self.args.report,
                summary_restrict=self.args.summary_restrict,
                archive=self.args.archive,
                postproc=self.args.postproc,
                postproc_data=self.args.postproc_data,
                sort=self.args.sort,
                targetfile=self.args.targetfile,
        )

        if self.args.stdin:
            arki_query.query_stdin(self.args.stdin)
        elif self.args.merged:
            arki_query.query_merged()
        elif self.args.qmacro:
            arki_query.query_qmacro(self.args.qmacro, qmacro_query)
        else:
            arki_query.query_sections()
