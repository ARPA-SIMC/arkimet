from __future__ import annotations
import arkimet as arki
from arkimet.cmdline.base import App, AppConfigMixin, Fail
import sys
import logging


class Mergeconf(AppConfigMixin, App):
    """
    Read dataset configuration from the given directories or config files,
    merge them and output the merged config file to standard output
    """
    log = logging.getLogger("arki-mergeconf")

    def __init__(self):
        super().__init__()
        self.parser.add_argument("-o", "--output", metavar="file",
                                 help="write the output to the given file instead of standard output")
        self.parser.add_argument("--extra", action="store_true",
                                 help="extract extra information from the datasets (such as bounding box)"
                                      " and include it in the configuration")
        self.parser.add_argument("--ignore-system-datasets", action="store_true",
                                 help="ignore error and duplicates datasets")
        self.parser.add_argument("--restrict", metavar="names", action="store",
                                 help="restrict operations to only those datasets that allow one of the given"
                                      " (comma separated) names")
        self.parser.add_argument("-C", "--config", metavar="file", action="append",
                                 help="merge configuration from the given file (can be given more than once)")
        self.parser.add_argument("sources", nargs="*", action="store",
                                 help="datasets, configuration files or remote urls")

    def run(self):
        super().run()

        # Process --config options
        if self.args.config is not None:
            for pathname in self.args.config:
                cfg = arki.dataset.read_configs(pathname)
                for name, section in cfg.items():
                    self.add_config_section(section, name)

        # Read the config files from the remaining commandline arguments
        for path in self.args.sources:
            if path.startswith("http://") or path.startswith("https://"):
                sections = arki.dataset.http.load_cfg_sections(path)
                for name, section in sections.items():
                    self.add_config_section(section, name)
            else:
                section = arki.dataset.read_config(path)
                self.add_config_section(section)

        if not self.session.has_datasets():
            if self.config_filter_discarded:
                raise Fail("none of the configuration provided were useable")
            raise Fail("you need to specify at least one config file or dataset")

        # Validate the configuration
        has_errors = False
        for dataset in self.session.datasets():
            # Validate filters
            filter = dataset.config.get("filter")
            if filter is None:
                continue

            try:
                self.session.matcher(filter)
            except ValueError as e:
                print("{}: {}".format(name, e), file=sys.stderr)
                has_errors = True
        if has_errors:
            raise Fail("Some input files did not validate.")

        # If requested, compute extra information
        if self.args.extra:
            for dataset in self.session.datasets():
                with dataset.reader() as reader:
                    # Get the summary
                    summary = reader.query_summary()
                    # Compute bounding box, and store the WKT in bounding
                    bbox = summary.get_convex_hull()
                    if bbox:
                        dataset.config["bounding"] = bbox

        # Build the merged configuration
        config = arki.cfg.Sections()
        for dataset in self.session.datasets():
            config[dataset.name] = dataset.config

        # Output the merged configuration
        if self.args.output:
            with open(self.args.output, "wt") as fd:
                config.write(fd)
        else:
            config.write(sys.stdout)
