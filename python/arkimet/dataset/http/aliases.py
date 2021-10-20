from typing import Dict, Set
import arkimet as arki
import logging


class AliasMerger:
    """
    Fetch and merge alias databases from different servers
    """
    # TODO: unittest

    def __init__(self, log: logging.Logger):
        self.log = log
        self.aliases = arki.cfg.Sections()

    def fetch_aliases(self, sections: arki.cfg.Sections):
        """
        Fetch alias databases from remote servers, check that they are consistent,
        and merge them into a single one
        """
        # Fetch aliases from all servers
        by_server: Dict[str, arki.cfg.Sections] = {}
        for name, section in sections.items():
            server = section["server"]
            if server in by_server:
                continue
            by_server[server] = arki.dataset.http.get_alias_database(server)

    def merge_servers(self, by_server: Dict[str, arki.cfg.Sections]):
        """
        Merge all downloaded alias databases into a single one
        """
        # Build a list of all sections found in all alias databases
        all_sections: Set[str] = set()
        for sections in by_server.values():
            all_sections.update(sections.keys())

        # Process all sections found in all server alias databases
        for secname in all_sections:
            sections = Dict[str, arki.cfg.Section]
            for server, sections in by_server.items():
                section = sections.get(secname)
                if section is None:
                    # If a server doesn't have a section, warn and remove it
                    # from the merged database
                    self.log.warning(
                        "server %s: no aliases found in section [%s]."
                        " Skipping this section of aliases in the merged database",
                        server, secname)
                    break
                else:
                    sections[server] = section
            else:
                self.aliases[secname] = self.merge_sections(secname, sections)

    def merge_sections(self, secname: str, sections: Dict[str, arki.cfg.Section]):
        """
        Merge alias sections from different servers.

        If an entry is not defined in all servers, warn about it and do not
        include it in the merged set.

        If an entry has different definitions between servers, raise an
        exception.
        """
        def log_definitions(name: str):
            """
            Log all definitions for an alias in all servers
            """
            for server, aliases in sections.items():
                val = aliases.get(name)
                self.log.warning("Server %s: [%s] %s = %r", server, secname, name, val)

        skipped: Set[str] = set()
        conflict: Set[str] = set()

        merged = None
        for server, aliases in sections.items():
            if merged is None:
                merged = aliases.copy()
            else:
                for name, value in aliases.items():
                    old = merged.get(name)
                    if old is None:
                        skipped.add(name)
                    elif old != value:
                        conflict.add(name)

                for name in merged.keys():
                    if name not in aliases:
                        skipped.add(name)
                        del merged[name]

        for name in skipped - conflict:
            log_definitions(name)
            self.log.warning("[%s] %s: skipped as not available in all servers", secname, name)

        for name in conflict:
            log_definitions(name)
            self.log.error("[%s] %s: not all servers have the same value", secname, name)

        if conflict:
            raise RuntimeError(f"[{secname}]: alias mismatches between servers")

        return merged
