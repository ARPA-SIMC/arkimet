import datetime as dt
import logging
import math
from collections import defaultdict
from pathlib import Path
from typing import Callable, Dict, List, Tuple, Iterable, IO, Optional, Any

import wreport
import arkimet
import dballe

log = logging.getLogger("arkimet.scan.bufr")


class Scanner:
    """
    Legacy python engine for scanning BUFR files.

    This is a proxy for BUFRScanner, used so that legacy scanner plugins can
    register themselves.
    """

    @classmethod
    def register(
        cls,
        type: str,
        scanner: Callable[[dballe.Message, arkimet.Metadata], None],
        priority=0,
    ) -> None:
        """Register a scanner plugin."""
        bufr_scanner.register(type, scanner, priority)


class BUFR:
    """Hold the parsed parts of a BUFR message."""

    def __init__(self, binmsg: dballe.BinaryMessage) -> None:
        #: Raw binary message
        self.binmsg = binmsg
        #: Decoded message header
        self.bulletin: wreport.Bulletin = wreport.Bulletin.decode_bufr_header(
            bytes(binmsg), fname=binmsg.pathname, offset=binmsg.offset
        )
        #: Interpreted message subsets
        self.msgs: Optional[Tuple[dballe.Message]] = None

    def get_reftime(self) -> Optional[dt.datetime]:
        """Get the reference time for the message."""
        # Set the reference time from the message header
        # The header date should always be ignored, but we need a baseline
        # reftime for BUFR with multiple subsets or whose data cannot be
        # interpreted
        reftime: Optional[dt.datetime]
        try:
            reftime = self.bulletin.reftime
        except Exception as exc:
            log.warning(
                "%s+%s: BUFR has an invalid reference time: %s",
                self.binmsg.pathname,
                self.binmsg.offset,
                exc,
            )
            reftime = None

        if not self.msgs or len(self.msgs) != 1:
            return reftime

        # Refine reftime from the interpreted message datetime
        try:
            reftime = self.msgs[0].datetime
        except Exception as exc:
            log.warning(
                "%s+%s: BUFR subset does not contain valid datetime information: %s",
                self.binmsg.pathname,
                self.binmsg.offset,
                exc,
            )
        return reftime

    def get_origin(self) -> Optional[Dict[str, Any]]:
        """Get the origin from the BUFR header."""
        if self.bulletin.edition_number in (2, 3, 4):
            return {
                "style": "BUFR",
                "centre": self.bulletin.originating_centre,
                "subcentre": self.bulletin.originating_subcentre,
            }
        return None

    def get_product(self) -> Dict[str, Any]:
        """Get the product from message information."""

        def bufr_byte(val: Optional[int]) -> int:
            if val is None:
                return 255
            return val

        # Set base product information from the BUFR header
        product = {"style": "BUFR"}
        product["type"] = bufr_byte(self.bulletin.data_category)
        product["subtype"] = bufr_byte(self.bulletin.data_subcategory)
        product["local_subtype"] = bufr_byte(
            self.bulletin.data_subcategory_local
        )

        # Refine product information from the interpreted message type
        if self.msgs and len(self.msgs) == 1:
            product["value"] = {"t": self.msgs[0].type}

        return product


class BUFRScanner(arkimet.scan.DataFormatScanner):
    """
    Scanner for BUFR files.

    This is registered with the arkimet library as the entry point for managing
    BUFR data.

    Scanner configuration scripts for BUFR files need to register with this
    class to be executed.
    """

    def __init__(self) -> None:
        """Initialize the BUFRScanner with an empty plugin list."""
        #: Registered scanner plugins by BUFR edition number
        self.by_type: Dict[
            str,
            List[
                Tuple[int, Callable[[dballe.Message, arkimet.Metadata], None]]
            ],
        ] = defaultdict(list)
        self.scanners_sorted: bool = True
        #: DB-All.e importer that performs interpretation of raw BUFR contents
        self.importer = dballe.Importer(
            "BUFR", simplified=True, domain_errors="unset"
        )

    def get_scanners(
        self, type: str
    ) -> Optional[
        List[Tuple[int, Callable[[dballe.Message, arkimet.Metadata], None]]]
    ]:
        if not self.scanners_sorted:
            for scanners in self.by_type.values():
                scanners.sort(key=lambda s: s[0])
        return self.by_type.get(type)

    def scan_bufr(self, bufr: BUFR, md: arkimet.Metadata) -> None:
        """Scan a BUFR from available (possibly partial) information."""
        # Fill with generally available information
        reftime = bufr.get_reftime()
        if reftime is not None:
            md["reftime"] = {"style": "POSITION", "time": reftime}

        origin = bufr.get_origin()
        if origin is not None:
            md["origin"] = origin

        md["product"] = bufr.get_product()

        if not bufr.msgs or len(bufr.msgs) != 1:
            return

        # Complete with scanner plugins
        msg = bufr.msgs[0]

        # Find the formatter list for this style
        scanners = self.get_scanners(msg.type)
        if scanners is None:
            return

        # Try all scanner functions in the list, in priority order, stopping at
        # the first one that returns False.
        # Note that False is explicitly required: returning None will not stop
        # the scanner chain.
        for prio, scan in scanners:
            try:
                if scan(msg, md) is False:
                    break
            except Exception:
                log.warning("scanner function failed", exc_info=True)

        # // Check that the date is a valid date, unset if it is rubbish
        # const reftime::Position* rt = md->get<types::reftime::Position>();
        # if (rt)
        # {
        #     auto time = rt->get_Position();
        #     if (time.ye <= 0)
        #         md->unset(TYPE_REFTIME);
        #     else
        #     {
        #         core::Time t = time;
        #         t.normalise();
        #         if (t != time)
        #             md->unset(TYPE_REFTIME);
        #     }
        # }

        # Convert validity times to emission times
        md.convert_validity_time_to_emission_time()

    def scan_binmsg(
        self, binmsg: dballe.BinaryMessage, as_inline: bool = False
    ) -> arkimet.Metadata:
        if as_inline:
            md = arkimet.Metadata.for_bytes("bufr", bytes(binmsg))
        else:
            md = arkimet.Metadata.for_blob(
                "bufr", binmsg.pathname, binmsg.offset, bytes(binmsg)
            )
        try:
            bufr = BUFR(binmsg)
        except Exception as exc:
            log.warning(
                "%s+%s: cannot decode BUFR header: %s",
                binmsg.pathname,
                binmsg.offset,
                exc,
            )
            return md

        try:
            bufr.msgs = self.importer.from_binary(binmsg)
        except Exception as exc:
            log.warning(
                "%s+%s: interpretation of BUFR contents failed: %s",
                binmsg.pathname,
                binmsg.offset,
                exc,
            )

        self.scan_bufr(bufr, md)

        return md

    def scan_file(self, path: Path) -> Iterable[arkimet.Metadata]:
        """Scan a BUFR file."""
        with dballe.File(path.as_posix(), "BUFR") as binmsgs:
            for binmsg in binmsgs:
                yield self.scan_binmsg(binmsg)

    def scan_pipe(self, file: IO[bytes]) -> Iterable[arkimet.Metadata]:
        """Scan BUFR data from a pipe."""
        with dballe.File(file, "BUFR") as binmsgs:
            for binmsg in binmsgs:
                yield self.scan_binmsg(binmsg, as_inline=True)

    def scan_data(self, data: bytes) -> arkimet.Metadata:
        """Scan a data buffer as BUFR."""
        binmsg = dballe.BinaryMessage(data=data, encoding="BUFR")
        return self.scan_binmsg(binmsg, as_inline=True)

    def update_sequence_number(self, md: arkimet.Metadata) -> int:
        """Extract the update sequence number from BUFR data."""
        bulletin = wreport.Bulletin.decode_bufr_header(md.data)
        return bulletin.update_sequence_number

    def register(
        self,
        type: str,
        scanner: Callable[[dballe.Message, arkimet.Metadata], None],
        priority: int = 0,
    ) -> None:
        """
        Register a callable as an additional scanner.

        :param scanner: the callable to run to scan extra data from a BUFR
        :param priority: the order of execution (lower numbers run first)
        """
        if scanner not in self.by_type[type]:
            self.by_type[type].append((priority, scanner))
            self.scanners_sorted = False


#: Main BUFRScanner instance that handles scanning, and that plugins can use to
#: register themselves
bufr_scanner = BUFRScanner()

# Register bufr the arkimet library
arkimet.scan.registry.register_scanner("bufr", bufr_scanner)


def read_area_fixed(msg: dballe.Message) -> Optional[Dict[str, Any]]:
    """
    Read the area from a BUFR containing a fixed station
    """
    lat = msg.get_named("latitude")
    lon = msg.get_named("longitude")
    if lat is not None and lon is not None:
        return {"lat": lat.enqi(), "lon": lon.enqi()}
    else:
        return None


def read_area_mobile(msg: dballe.Message) -> Optional[Dict[str, Any]]:
    """
    Read the area from a BUFR containing a mobile station
    """
    lat = msg.get_named("latitude")
    lon = msg.get_named("longitude")
    if lat is not None and lon is not None:
        # Approximate to 1 degree to allow arkimet to perform grouping
        return {
            "type": "mob",
            "x": math.floor(lon.enqd()),
            "y": math.floor(lat.enqd()),
        }
    else:
        return None


def read_proddef(msg: dballe.Message) -> Optional[Dict[str, Any]]:
    """
    Read a default proddef value for a message
    """
    res: Dict[str, Any] = {}
    blo = msg.get_named("block")
    sta = msg.get_named("station")
    id = msg.get_named("ident")
    found = False
    if blo:
        res["blo"] = blo.enqi()
        found = True
    if sta:
        res["sta"] = sta.enqi()
        found = True
    if id:
        res["id"] = id.enqc()
        found = True
    if found:
        return res
    else:
        return None


# function bufr_scan_forecast(msg)
#     local forecast = nil
#     msg:foreach(function(v)
#         if v.pind == 254 then
#             -- Verify that all p1 are the same: if they differ, error()
#             local p1 = v.p1
#             if forecast ~= nil and forecast ~= p1 then
#                 error("BUFR message has contradictory forecasts"..tostring(forecast).." and "..tostring(p1))
#             end
#             forecast = p1
#         end
#     end)
#     if forecast == nil then forecast = 0 end
#
#     if forecast == 0 then
#         return arki_timerange.timedef(0)
#     elseif math.floor(forecast / 3600) * 3600 == forecast then
#         -- If it is a multiple of one hour, represent it in hours
#         return arki_timerange.timedef(forecast / 3600, "h")
#     elseif math.floor(forecast / 60) * 60 == forecast then
#         -- If it is a multiple of one minute, represent it in minutes
#         return arki_timerange.timedef(forecast / 60, "m")
#     else
#         return arki_timerange.timedef(forecast, "s")
#     end
# end
