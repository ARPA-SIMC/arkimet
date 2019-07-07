import arkimet as arki
from arkimet.cmdline.base import App, Exit
import re
import logging

log = logging.getLogger("arki-mergeconf")

re_stringlist = re.compile(r"[\s,]+")


class BufrPrepare(App):
    """
    Read BUFR messages, and encode each subsection in a separate message.
    """
    NAME = "arki-bufr-prepare"

    def __init__(self):
        super().__init__()
        self.parser.add_argument("-o", "--output", metavar="file",
                                 help="write the output to the given file instead of standard output")
        self.parser.add_argument("--fail", metavar="file",
                                 help="do not ignore BUFR data that could not be decoded,"
                                      " but write it to the given file")
        self.parser.add_argument("--usn", metavar="number",
                                 help="overwrite the update sequence number of every BUFR message with this value")
        self.parser.add_argument("files", nargs="*", action="store",
                                 help="BUFR files to process")

    def run(self):
        super().run()

        cmd = arki.ArkiBUFRPrepare()
        res = cmd.run(
            inputs=self.args.files,
            usn=self.args.usn,
            outfile=self.args.output,
            failfile=self.args.fail,
        )
        if res:
            raise Exit(res)
