from arkimet.formatter import Formatter
from arkimet.formatter.eccodes import GribTable

timedef_units = {
    0: "minute",
    1: "hour",
    2: "day",
    3: "month",
    4: "year",
    5: "decade",
    6: "30years",
    7: "century",
    10: "3hours",
    11: "6hours",
    12: "12hours",
    13: "second",
    255: "-",
}

timedef_prefixes = {
    0: "Average",
    1: "Accumulation",
    2: "Maximum",
    3: "Minimum",
    4: "Difference (end minus beginning)",
    5: "Root Mean Square",
    6: "Standard Deviation",
    7: "Covariance (temporal variance)",
    8: "Difference (beginning minus end)",
    9: "Ratio",
    10: "Standardized anomaly",
    200: "Vectorial mean",
    201: "Mode",
    202: "Standard deviation vectorial mean",
    203: "Vectorial maximum",
    204: "Vectorial minimum",
    205: "Product with a valid time ranging",
    255: "No statistical processing",
}


def format_timerange(v):
    if v["style"] == "GRIB1":
        timerange = GribTable.load(1, "5")
        if not timerange.has(v["trange_type"]):
            return None
        desc = timerange.desc(v["trange_type"]) + " - "
        if v["p1"]:
            desc += "p1 {}".format(v["p1"])
        if v["p2"]:
            desc += "p2 {}".format(v["p2"])
        if v["unit"]:
            desc += "time unit {}".format(v["unit"])
        return desc
    elif v["style"] == "Timedef":
        step_unit, step_len, stat_type, stat_unit, stat_len = (
            v.get("step_unit", 255),
            v.get("step_len", 0),
            v.get("stat_type", 255),
            v.get("stat_unit", 255),
            v.get("stat_len", 0),
        )

        if stat_type == 254:
            if step_len == 0 and stat_len == 0:
                return "Analysis or observation, istantaneous value"
            else:
                return "Forecast at t+{} {}, instantaneous value".format(step_len, timedef_units.get(step_unit, "-"))
        else:
            prefix = timedef_prefixes.get(stat_type)
            if prefix is None:
                raise RuntimeError("Unknown statistical processing {}".format(stat_type))
            elif step_len is None and stat_len is None:
                return prefix
            else:
                if stat_len is not None:
                    prefix = "{} over {} {}".format(prefix, stat_len, timedef_units.get(stat_unit, "-"))
                if step_len is None:
                    return prefix
                elif step_len < 0:
                    return "{} {} {} before reference time".format(
                            prefix, (-1 * step_len), timedef_units.get(step_unit, "-"))
                else:
                    return "{} at forecast time {} {}".format(prefix, step_len, timedef_units.get(step_unit, "-"))


Formatter.register("timerange", format_timerange)
