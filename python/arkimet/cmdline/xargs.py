import arkimet
from arkimet.cmdline.base import App, Exit
import argparse
import logging

log = logging.getLogger("arki-xargs")


class Xargs(App):
    """
    For every item of data read from standard input, save it on a temporary
    file and run 'command [initial-arguments] filename' on it
    """
    NAME = "arki-xargs"

    @classmethod
    def make_parser(cls) -> argparse.ArgumentParser:
        parser = super().make_parser()
        parser.add_argument("-i", "--input", metavar="file", action="append",
                            help="read data from this file(s) instead of standard input"
                                 " (can be given more than once)")
        parser.add_argument("--max-args", "-n", metavar="count", type=int,
                            help="group at most this amount of data items per command invocation")
        parser.add_argument("--max-size", "-s", metavar="size",
                            help="create data files no bigger than this size. This may NOT be respected"
                                 " if there is one single data item greater than the size specified."
                                 " size may be followed by SI or IEC multiplicative suffixes: b,c=1,"
                                 "K,Kb=1000, Ki=1024, M,Mb=1000*1000, Mi=1024*1024, G,Gb=1000*1000*1000,"
                                 "Gi=1024*1024*1024, and so on for T, P, E, Z, Y")
        parser.add_argument("--time-interval", metavar="interval",
                            help="create one data file per 'interval', where interval can be minute,"
                                 " hour, day, month or year")
        parser.add_argument("--split-timerange", action="store_true",
                            help="start a new data file when the time range changes")
        parser.add_argument("command", nargs=argparse.REMAINDER,
                            help="command to run, including initial arguments")
        return parser

    def __sanitize_command(self, command):
        """Ignore double dash if it's the first item in the command list."""
        return command[1:] if command and command[0] == "--" else command

    def run(self):
        super().run()
        res = 0
        cmd = arkimet.cmdline.ArkiXargs()
        res = cmd.run(
            command=self.__sanitize_command(self.args.command),
            inputs=self.args.input,
            max_args=self.args.max_args,
            max_size=self.parse_size(self.args.max_size) if self.args.max_size else 0,
            time_interval=self.args.time_interval,
            split_timerange=self.args.split_timerange,
        )
        if res:
            raise Exit(res)
