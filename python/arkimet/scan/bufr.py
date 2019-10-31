from typing import Callable
from collections import defaultdict
import arkimet
import dballe
import math
import logging

log = logging.getLogger("arkimet.scan.bufr")


class Scanner:
    by_type = defaultdict(list)

    def __init__(self):
        for type, scanners in self.by_type.items():
            scanners.sort()

    def scan(self, msg: dballe.Message, md: arkimet.Metadata):
        # Find the formatter list for this style
        scanners = self.by_type.get(msg.type)
        if scanners is None:
            return None

        # Try all scanner functions in the list, in priority order, stopping at
        # the first one that returns False.
        # Note that False is explicitly required: returning None will not stop
        # the scanner chain.
        for prio, scan in scanners:
            try:
                if scan(msg, md) is False:
                    break
            except Exception:
                log.exception("scanner function failed")

    @classmethod
    def register(cls, type: str, scanner: Callable[[dballe.Message, arkimet.Metadata], None], priority=0):
        if scanner not in cls.by_type[type]:
            cls.by_type[type].append((priority, scanner))


def read_area_fixed(msg):
    """
    Read the area from a BUFR containing a fixed station
    """
    lat = msg.get_named('latitude')
    lon = msg.get_named('longitude')
    if lat is not None and lon is not None:
        return {"lat": lat.enqi(), "lon": lon.enqi()}
    else:
        return None


def read_area_mobile(msg):
    """
    Read the area from a BUFR containing a mobile station
    """
    lat = msg.get_named('latitude')
    lon = msg.get_named('longitude')
    if lat is not None and lon is not None:
        # Approximate to 1 degree to allow arkimet to perform grouping
        return {
            "type": "mob",
            "x": math.floor(lon.enqd()),
            "y": math.floor(lat.enqd())
        }
    else:
        return None


def read_proddef(msg):
    """
    Read a default proddef value for a message
    """
    res = {}
    blo = msg.get_named('block')
    sta = msg.get_named('station')
    id = msg.get_named('ident')
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
