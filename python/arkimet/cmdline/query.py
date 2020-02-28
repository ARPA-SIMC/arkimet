import arkimet
from arkimet.cmdline.base import AppConfigMixin, AppWithProcessor
import sys
import logging
try:
    import progressbar
except ModuleNotFoundError:
    progressbar = None


class Progress:
    def __init__(self):
        self.fd = sys.stderr
        self.pbar = None
        self.total_bytes = 0

    def start(self, expected_count=0, expected_bytes=0):
        if progressbar is not None:
            self.pbar = progressbar.ProgressBar(
                    maxval=expected_bytes or progressbar.widgets.UnknownLength,
                    fd=self.fd, widgets=[
                        progressbar.Timer(), " ",
                        progressbar.AnimatedMarker(), " ",
                        progressbar.Counter(), " ",
                        progressbar.FileTransferSpeed(),
                    ])
            self.pbar.start()
        else:
            self.pbar = None

    def update(self, count, bytes):
        self.total_bytes += bytes
        if self.pbar is not None:
            self.pbar.update(self.total_bytes)

    def done(self, total_count, total_bytes):
        if self.pbar is not None:
            self.pbar.finish()


class Query(AppConfigMixin, AppWithProcessor):
    """
    Query the datasets in the given config file for data matching the given
    expression, and output the matching metadata.
    """
    log = logging.getLogger("arki-query")

    def __init__(self):
        super().__init__()

        # Inputs
        self.parser.add_argument("--stdin", metavar="format",
                                 help="read input from standard input in the given format")
        self.parser.add_argument("query", nargs="?",
                                 help="arkimet query to run")
        self.parser.add_argument("source", nargs="*",
                                 help="input files or datasets")

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
                                      " --postprocess or --data")

        self.parser.add_argument("--file", "-f", metavar="file",
                                 help="read the query expression from the given file")

        self.parser.add_argument("--qmacro", metavar="name",
                                 help="run the given query macro instead of a plain query")
        self.parser.add_argument("--progress", action="store_true",
                                 help="show a progress bar on stderr")

    def build_config(self):
        self.query = None
        self.sources = []
        if self.args.file is not None:
            with open(self.args.file, "rt") as fd:
                self.query = fd.read()
            if self.args.query is not None:
                self.sources = [self.args.query] + self.args.source
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
                    cfg = arkimet.dataset.read_configs(pathname)
                    for name, section in cfg.items():
                        self.add_config_section(section, name)

            # From command line arguments, looking for data files or datasets
            for source in self.sources:
                cfg = arkimet.dataset.read_configs(source)
                for name, section in cfg.items():
                    self.add_config_section(section, name)

            if not self.config:
                self.parser.error("you need to specify at least one input file or dataset")

            # Remove unallowed entries
            if self.args.restrict:
                self.filter_restrict(self.args.restrict)

            if not self.config:
                self.parser.error("no accessible datasets found for the given --restrict value")

            # Some things cannot be done when querying multiple datasets at the same time
            if len(self.config) > 1 and not self.args.qmacro and self.args.postproc:
                self.parser.error(
                        "postprocessing is not possible when querying more than one dataset at the same time")

    def run(self):
        super().run()

        self.build_config()

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
            query = arkimet.Matcher(arkimet.dataset.http.expand_remote_query(self.config, strquery))
        else:
            query = arkimet.Matcher()

        with self.outfile() as outfd:
            arki_query = arkimet.cmdline.ArkiQuery()
            arki_query.set_inputs(self.config)
            progress = None
            if self.args.progress:
                progress = Progress()
            arki_query.set_processor(
                    query=query,
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
                    progress=progress,
            )

            if self.args.stdin:
                arki_query.query_file(sys.stdin, self.args.stdin)
            elif self.args.merged:
                arki_query.query_merged()
            elif self.args.qmacro:
                arki_query.query_qmacro(self.args.qmacro, qmacro_query)
            else:
                arki_query.query_sections()
