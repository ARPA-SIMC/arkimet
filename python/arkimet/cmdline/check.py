from __future__ import annotations
import arkimet
from arkimet.cmdline.base import App, AppConfigMixin
import logging


class Check(AppConfigMixin, App):
    """
    Given one or more dataset root directories (either specified directly or
    read from config files), perform a maintenance run on them.

    Corrupted metadata files will be rebuilt, data files with deleted data
    will be packed, outdated summaries and indices will be regenerated.
    """
    log = logging.getLogger("arki-check")

    def __init__(self):
        super().__init__()
        actions = self.parser.add_mutually_exclusive_group()
        actions.add_argument("--repack", "-r", action="store_true",
                             help="Perform a repack instead of a check")
        actions.add_argument("--remove-old", action="store_true",
                             help="Remove data older than delete age")
        actions.add_argument("--remove-all", action="store_true",
                             help="Completely empty the dataset(s), removing all data and metadata")
        actions.add_argument("--tar", action="store_true",
                             help="Convert directory segments into tar files")
        actions.add_argument("--zip", action="store_true",
                             help="Convert directory segments into zip files")
        actions.add_argument("--compress", action="store", type=int, nargs="?", metavar="group_size", const=512,
                             help="Compress file segments, group_size data elements in each"
                                  " compressed block. Default is 512, use 0 for plain gzip without"
                                  " index.")
        actions.add_argument("--remove", action="store", metavar="file",
                             help="Given metadata extracted from one or more datasets,"
                                  " remove it from the datasets where it is stored")
        actions.add_argument("--unarchive", action="store", metavar="pathname",
                             help="Given a pathname relative to .archive/last, move it out of the archive"
                                  " and back to the main dataset")
        actions.add_argument("--state", action="store_true",
                             help="Scan the dataset and print its state")
        actions.add_argument("--issue51", action="store_true",
                             help="Check a dataset for corrupted terminator bytes (see issue #51)")

        self.parser.add_argument("--config", "-C", metavar="file", action="append",
                                 help="read configuration about datasets to scan from the given file"
                                      " (can be given more than once)")
        self.parser.add_argument("--restrict", action="store", metavar="names",
                                 help="Restrict operations to only those datasets that allow"
                                      " one of the given (comma separated) names")
        self.parser.add_argument("--fix", "-f", action="store_true",
                                 help="Perform the changes instead of writing what would have been changed")
        self.parser.add_argument("--accurate", action="store_true",
                                 help="Also verify the consistency of the contents of the data files (slow)")
        self.parser.add_argument("--filter", action="store", metavar="matcher",
                                 help="Restrict operations to at least those segments whose reference time"
                                      " matches the given matcher (but some more datasets may be included)")
        self.parser.add_argument("--online", action="store_true",
                                 help="Work on online data, skipping archives unless --offline is also provided")
        self.parser.add_argument("--offline", action="store_true",
                                 help="Work on archives, skipping online data unless --online is also provided")

        self.parser.add_argument("dataset", nargs="*",
                                 help="dataset(s) to check")

    def build_config(self):
        """
        Fill the input datasets configuration
        """
        # From -C options, looking for config files or datasets
        if self.args.config:
            for pathname in self.args.config:
                cfg = arkimet.dataset.read_configs(pathname)
                for name, section in cfg.items():
                    self.add_config_section(section, name)

        # From command line arguments, looking for data files or datasets
        for dataset in self.args.dataset:
            section = arkimet.dataset.read_config(dataset)
            self.add_config_section(section)

        if not self.session.has_datasets():
            self.parser.error("you need to specify at least one dataset to work on")

        # Remove unallowed entries
        if self.args.restrict:
            self.filter_restrict(self.args.restrict)

        if not self.session.has_datasets():
            self.parser.error("no accessible datasets found for the given --restrict value")

    def run(self):
        super().run()
        self.build_config()

        # Set defaults for online and offline depending on the operation performed
        if not self.args.online and not self.args.offline:
            if self.args.remove_all or self.args.remove_old or self.args.repack:
                offline = False
                online = True
            elif self.args.tar or self.args.zip or self.args.compress:
                offline = True
                online = False
            else:
                offline = online = True
        else:
            offline = self.args.offline
            online = self.args.online

        arki_check = arkimet.cmdline.ArkiCheck(
                self.session,
                filter=self.args.filter,
                accurate=self.args.accurate,
                online=online,
                offline=offline,
                readonly=not self.args.fix,
        )
        if self.args.remove:
            arki_check.remove(self.args.remove)
        elif self.args.remove_all:
            arki_check.remove_all()
        elif self.args.remove_old:
            arki_check.remove_old()
        elif self.args.repack:
            arki_check.repack()
        elif self.args.tar:
            arki_check.tar()
        elif self.args.zip:
            arki_check.zip()
        elif self.args.compress is not None:
            arki_check.compress(group_size=self.args.compress)
        elif self.args.unarchive is not None:
            arki_check.unarchive(pathname=self.args.unarchive)
        elif self.args.state:
            arki_check.state()
        elif self.args.issue51:
            arki_check.check_issue51()
        else:
            arki_check.check()
