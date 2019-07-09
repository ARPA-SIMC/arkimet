import arkimet as arki
from arkimet.cmdline.base import App, Exit
import sys
import logging

log = logging.getLogger("arki-dump")


class Dump(App):
    """
    Read data from the given input file (or stdin), and dump the in human readable format on stdout
    """

    def __init__(self):
        super().__init__()
        self.parser.add_argument("-o", "--output", metavar="file",
                                 help="write the output to the given file instead of standard output")
        self.parser.add_argument("input", action="store", nargs="?",
                                 help="file to read (or stdin if omitted)")

        group = self.parser.add_mutually_exclusive_group()
        group.add_argument("--query", action="store_true",
                           help="print a query (specified on the command line) with the aliases expanded")
        group.add_argument("--config", action="store_true",
                           help="print the arkimet configuration used to access the given file or dataset or URL")
        group.add_argument("--aliases", action="store_true",
                           help="dump the alias database (to dump the aliases of a remote server,"
                                " put the server URL on the command line)")
        group.add_argument("--info", action="store_true",
                           help="dump configuration information")
        group.add_argument("--bbox", action="store_true",
                           help="dump the bounding box")
        group.add_argument("--from-yaml-data", action="store_true",
                           help="read a Yaml data dump and write binary metadata")
        group.add_argument("--from-yaml-summary", action="store_true",
                           help="read a Yaml summary dump and write a binary summary")
        group.add_argument("--annotate", action="store_true",
                           help="annotate the human-readable Yaml output with field descriptions")

    def run(self):
        super().run()

        if self.args.query:
            if not self.args.input:
                self.parser.error("--query needs a query on the command line")
            matcher = arki.Matcher(self.args.input)
            print(matcher.expanded)
            raise Exit()

        if self.args.aliases:
            if self.args.input:
                sections = arki.dataset.http.get_alias_database(self.args.input)
            else:
                sections = arki.get_alias_database()

            if self.args.output:
                with open(self.args.output, "wt") as fd:
                    sections.write(fd)
            else:
                sections.write(sys.stdout)
            raise Exit()

        if self.args.config:
            self.parser.error("please use arki-mergeconf instead of arki-dump --config")

        if self.args.info:
            import json
            cfg = arki.config()
            print(json.dumps(cfg, indent=1))
            raise Exit()

        if self.args.bbox:
            dump = arki.ArkiDump()
            raise Exit(dump.bbox(self.args.input or "-", self.args.output or "-"))

        if self.args.from_yaml_data:
            dump = arki.ArkiDump()
            raise Exit(dump.reverse_data(self.args.input or "-", self.args.output or "-"))

        if self.args.from_yaml_summary:
            dump = arki.ArkiDump()
            raise Exit(dump.reverse_summary(self.args.input or "-", self.args.output or "-"))

        dump = arki.ArkiDump()
        raise Exit(dump.dump_yaml(self.args.input or "-", self.args.output or "-", self.args.annotate))
