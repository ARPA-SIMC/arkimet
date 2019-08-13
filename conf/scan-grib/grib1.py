from arkimet.scan.grib import Scanner
from arkimet.scan import timedef

cosmo_centres = {78, 80, 200}

cosmo_nudging_table2 = {
    11:  0,  15: 2,  16: 3,  17: 0,  33: 0,  34: 0,
    57:  1,  61: 1,  78: 1,  79: 1,  90: 1, 111: 0,
    112: 0, 113: 0, 114: 0, 121: 0, 122: 0, 124: 0,
    125: 0, 126: 0, 127: 0, 152: 1, 153: 1
}
cosmo_nudging_table201 = {
    5:   0,  20: 1,  22: 0,  23: 0,  24: 0,  25: 0,
    26:  0,  27: 0,  42: 1, 102: 1, 113: 1, 132: 1,
    135: 1, 187: 2, 218: 2, 219: 2
}
cosmo_nudging_table202 = {
    231: 0, 232: 0, 233: 0
}


def scan_grib1(grib, md):
    # Reference time
# 	local year = (grib.centuryOfReferenceTimeOfData - 1) * 100 + grib.yearOfCentury
# 	local reftime = arki_time.time(year, grib.month, grib.day, grib.hour, grib.minute, grib.second)
# 	md:set(arki_reftime.position(reftime))

    # Run
# 	md:set(arki_run.minute(grib.hour, grib.minute))

    # Origin
    md["origin"] = {
        "style": "GRIB1",
        "centre": grib.get_long("centre"),
        "subcentre": grib.get_long("subCentre"),
        "process": grib.get_long("generatingProcessIdentifier"),
    }

    # Product
    md["product"] = {
        "style": "GRIB1",
        "origin": grib.get_long("centre"),
        "table": grib.get_long("gribTablesVersionNo"),
        "product": grib.get_long("indicatorOfParameter"),
    }

    # Level
    level = {
        "style": "GRIB1",
        "level_type": grib.get_long("indicatorOfTypeOfLevel"),
    }
    if grib["levels"] is None:
        level["l1"] = grib.get_long("level")
        level["l2"] = 0
    else:
        level["l1"] = grib.get_long("topLevel")
        level["l2"] = grib.get_long("bottomLevel")
    md["level"] = level

    # Create this array here so time range can add to it
    proddef = {}

    # Time range
    if grib.get_long("timeRangeIndicator") == 13 and grib.get_long("centre") in cosmo_centres:
        # COSMO 'nudging'
        # Special scan directly to timedef timeranges
        # needs to be done here since it depends on the parameter

        proddef["tod"] = 0  # Analysis product

        # Instantaneous data
        if grib["P1"] == 0 and grib["P2"] == 0:
            md["timerange"] = {
                "style": "Timedef",
                "step_len": 0,
                "step_unit": timedef.UNIT_SECOND,
                "stat_type": 254,
                "stat_len": 0,
                "stat_unit": timedef.UNIT_SECOND,
            }
        elif grib["P1"] > grib["P2"]:
            # TODO: this could be a general warning
            raise RuntimeError("COSMO message with P1 > P2")
        else:
            statproc = None

            # guess timerange according to parameter
            if grib.get_long("gribTablesVersionNo") == 2:
                # table 2
                statproc = cosmo_nudging_table2.get(grib.get_long("indicatorOfParameter"))
            elif grib.get_long("gribTablesVersionNo") == 201:
                # table 201
                statproc = cosmo_nudging_table201.get(grib.get_long("indicatorOfParameter"))
            elif grib.get_long("gribTablesVersionNo") == 202:
                # table 202
                statproc = cosmo_nudging_table202.get(grib.get_long("indicatorOfParameter"))
            # If gribTablesVersionNo is unsupported or if
            # gribl.indicatorOfParameter is not found in tables, statproc
            # is nil and later below we default to 'point in time' and
            # 'cosmo nudging analysis'

            if statproc is None:
                # default to point in time, with 'cosmo nudging analysis' as
                # statistical processing
                raise RuntimeError("Unknown COSMO parameter type")
            else:
                # known statistical processing
                tunit = grib.get_long("indicatorOfUnitOfTimeRange")
                if tunit == 254:
                    tunit = timedef.UNIT_SECOND
                md["timerange"] = {
                    "style": "Timedef",
                    "step_len": 0,
                    "step_unit": timedef.UNIT_SECOND,
                    "stat_type": statproc,
                    "stat_len": grib["P2"] - grib["P1"],
                    "stat_unit": tunit,
                }
    else:
        md["timerange"] = {
            "style": "GRIB1",
            "trange_type": grib["timeRangeIndicator"],
            "unit": grib["indicatorOfUnitOfTimeRange"],
            "p1": grib["P1"],
            "p2": grib["P2"],
        }
        proddef["tod"] = 1  # Initial instant of forecast

    # Area
    area = {
        "type": grib["dataRepresentationType"],
    }

    if grib["latitudeOfFirstGridPointInDegrees"] is not None:
        if grib["latitudeOfFirstGridPointInDegrees"] >= 1000:
            # UTM hack
            area["utm"] = 1

            area["latfirst"] = round(grib["latitudeOfFirstGridPointInDegrees"] * 1000)
            area["lonfirst"] = round(grib["longitudeOfFirstGridPointInDegrees"] * 1000)

            if grib["numberOfPointsAlongAParallel"]:
                area["Ni"] = round(grib["numberOfPointsAlongAParallel"])
                area["Nj"] = round(grib["numberOfPointsAlongAMeridian"])
                area["latlast"] = round(grib["latitudeOfLastGridPointInDegrees"] * 1000)
                area["lonlast"] = round(grib["longitudeOfLastGridPointInDegrees"] * 1000)
            if grib["numberOfPointsAlongXAxis"]:
                area["Ni"] = grib["numberOfPointsAlongXAxis"]
                area["Nj"] = grib["numberOfPointsAlongYAxis"]
        else:
            area["latfirst"] = round(grib["latitudeOfFirstGridPointInDegrees"] * 1000000)
            area["lonfirst"] = round(grib["longitudeOfFirstGridPointInDegrees"] * 1000000)

            if grib["numberOfPointsAlongAParallel"]:
                area["Ni"] = grib["numberOfPointsAlongAParallel"]
                area["Nj"] = grib["numberOfPointsAlongAMeridian"]
                area["latlast"] = round(grib["latitudeOfLastGridPointInDegrees"] * 1000000)
                area["lonlast"] = round(grib["longitudeOfLastGridPointInDegrees"] * 1000000)

            if grib["numberOfPointsAlongXAxis"]:
                area["Ni"] = grib["numberOfPointsAlongXAxis"]
                area["Nj"] = grib["numberOfPointsAlongYAxis"]

            if grib["angleOfRotationInDegrees"] is not None:
                area["rot"] = round(grib["angleOfRotationInDegrees"] * 1000000)
                area["latp"] = round(grib["latitudeOfSouthernPoleInDegrees"] * 1000000)
                area["lonp"] = round(grib["longitudeOfSouthernPoleInDegrees"] * 1000000)
    md["area"] = {
        "style": "GRIB",
        "value": area,
    }

    # Proddef
# 	if grib.localDefinitionNumber then
# 		proddef.ld = gribl.localDefinitionNumber
# 		proddef.mt = gribl.marsType
# 		if grib.clusterNumber then
# 			proddef.nn = gribl.clusterNumber
# 		end
# 		if grib.forecastProbabilityNumber then
# 			proddef.nn = gribl.forecastProbabilityNumber
# 		end
# 		if grib.perturbationNumber then
# 			proddef.nn = gribl.perturbationNumber
# 		end
# 	end
# 
# 	-- Only set proddef if there is some data in it
# 	if next(proddef) ~= nil then
# 		md:set(arki_proddef.grib(proddef))
# 	end


Scanner.register(1, scan_grib1)
