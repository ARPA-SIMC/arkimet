from arkimet.formatter import Formatter
from arkimet.formatter.eccodes import GribTable
import os

# Load code table 0: Identification of centers
grib1_centres = GribTable.load(1, "0")

# Amend the table with the ARPAE SIMC information
grib1_centres.set(200, "arpa", "ARPAE SIMC Emilia Romagna")


def format_origin(v):
    """
    Format an origin: return a string, or None to fall back to other
    formatters
    """
    if v["style"] == "GRIB1":
        return "GRIB1 from {}, subcentre {}, process {}".format(
                grib1_centres.desc(v["centre"]), v["subcentre"], v["process"])
    elif v["style"] == "GRIB2":
        # GRIB2 process types
        grib2_processtypes = GribTable.load(2, os.path.join("tables", "4", "4.3"))

        return "GRIB2 from {}, subcentre {}, type {}, background process {}, process {}".format(
            grib1_centres.desc(v["centre"]),
            v["subcentre"],
            grib2_processtypes.desc(v["process_type"]),
            v["background_process_id"],
            v["process_id"])


Formatter.register("origin", format_origin)
