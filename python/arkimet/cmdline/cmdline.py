# from __future__ import annotations
import argparse
import logging
import sys
from typing import Any, Optional, Type, cast

log = logging.getLogger("command")


def _get_first_docstring_line(obj: Any) -> Optional[str]:
    if obj.__doc__ is None:
        raise RuntimeError(f"{obj!r} lacks a docstring")
    try:
        return cast(str, obj.__doc__).split("\n")[1].strip()
    except (AttributeError, IndexError):
        return None


class Fail(BaseException):
    """
    Failure that causes the program to exit with an error message.

    No stack trace is printed.
    """
    pass


class Success(BaseException):
    """
    Exception raised when a command has been successfully handled, and no
    further processing should happen
    """
    pass


class Command:
    """
    Base class for actions run from command line
    """
    NAME: str
    argumentparser_class: Type[argparse.ArgumentParser] = argparse.ArgumentParser

    def __init__(self, parser: argparse.ArgumentParser, args: argparse.Namespace):
        self.NAME = getattr(self.__class__, "NAME", self.__class__.__name__.lower())
        self.parser = parser
        self.args = args
        self.setup_logging()

    def setup_logging(self) -> None:
        FORMAT = "%(asctime)-15s %(levelname)s %(name)s %(message)s"
        if self.args.debug:
            level = logging.DEBUG
        elif self.args.verbose:
            level = logging.INFO
        else:
            level = logging.WARN

        logging.basicConfig(level=level, stream=sys.stderr, format=FORMAT)

    @classmethod
    def add_logging_options(cls, parser):
        parser.add_argument("--verbose", "-v", action="store_true",
                            help="verbose output")
        parser.add_argument("--debug", action="store_true",
                            help="debug output")

    @classmethod
    def make_parser(cls) -> argparse.ArgumentParser:
        parser: argparse.ArgumentParser = cls.argumentparser_class(
            description=_get_first_docstring_line(cls),
        )
        cls.add_logging_options(parser)
        return parser

    @classmethod
    def make_subparser(cls, subparsers: "argparse._SubParsersAction[Any]") -> argparse.ArgumentParser:
        cls.NAME = getattr(cls, "NAME", cls.__name__.lower())
        parser: argparse.ArgumentParser = subparsers.add_parser(
            cls.NAME,
            help=_get_first_docstring_line(cls),
        )
        parser.set_defaults(command=cls)
        cls.add_logging_options(parser)
        return parser
