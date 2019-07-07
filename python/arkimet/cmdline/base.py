import argparse
import logging
import sys


class Fail(Exception):
    pass


class Exit(Exception):
    pass


class App:
    NAME = None

    def __init__(self):
        self.parser = argparse.ArgumentParser(description=self.get_description())
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
