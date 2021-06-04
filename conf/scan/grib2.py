from arkimet.scan.grib import Scanner
from arkimet.scan import timedef
import datetime


METEOSAT_NAMES = {
    55: "MSG1",
    56: "MSG2",
    57: "MSG3",
    70: "MSG4",
}

METEOSAT_CHAN_NAMES = {
    6:   "VIS006",
    8:   "VIS008",
    16:  "IR016",
    39:  "IR039",
    62:  "WV062",
    73:  "WV073",
    87:  "IR087",
    97:  "IR907",
    108: "IR108",
    120: "IR120",
    134: "IR134",
    7:   "HRV",
}


def scan_grib2(grib, md):
    # Reference time
    md["reftime"] = {
        "style": "POSITION",
        "time": datetime.datetime(grib["year"], grib["month"], grib["day"],
                                  grib["hour"], grib["minute"], grib["second"]),
    }

    # Run
    md["run"] = {
        "style": "MINUTE",
        "value": grib["hour"] * 60 + grib["minute"],
    }

    # Origin
    process_id = grib.get_long("generatingProcessIdentifier")
    if process_id is None:
        process_id = grib.get_long("observationGeneratingProcessIdentifier")
    background_process_id = grib.get_long("backgroundGeneratingProcessIdentifier")
    if background_process_id is None:
        background_process_id = 0xff
    md["origin"] = {
        "style": "GRIB2",
        "centre": grib.get_long("centre"),
        "subcentre": grib.get_long("subCentre"),
        "process_type": grib.get_long("typeOfGeneratingProcess"),
        "background_process_id": background_process_id,
        "process_id": process_id,
    }

    # Product
    md["product"] = {
        "style": "GRIB2",
        "centre": grib.get_long("centre"),
        "discipline": grib.get_long("discipline"),
        "category": grib.get_long("parameterCategory"),
        "number": grib.get_long("parameterNumber"),
        "table_version": grib.get_long("tablesVersion"),
        "local_table_version": grib.get_long("localTablesVersion"),
    }

    # Level
    ltype1, lscale1, lvalue1 = grib.get_long("typeOfFirstFixedSurface"), 0, 0
    if grib.get_long("scaleFactorOfFirstFixedSurface") != -1:
        lscale1 = grib.get_long("scaleFactorOfFirstFixedSurface")
        lvalue1 = grib.get_long("scaledValueOfFirstFixedSurface")

    if "typeOfSecondFixedSurface" in grib and grib["typeOfSecondFixedSurface"] != 255:
        level = {
            "style": "GRIB2D",
            "l1": ltype1, "scale1": lscale1, "value1": lvalue1,
            "l2": grib["typeOfSecondFixedSurface"],
        }
        if grib.get_long("scaleFactorOfSecondFixedSurface") != -1:
            level["scale2"] = grib["scaleFactorOfSecondFixedSurface"]
            level["value2"] = grib["scaledValueOfSecondFixedSurface"]
        else:
            level["scale2"] = 0
            level["value2"] = 0
        md["level"] = level
    else:
        md["level"] = {
            "style": "GRIB2S",
            "level_type": ltype1,
            "scale": lscale1,
            "value": lvalue1,
        }

    # Time range
    tr_ft = grib.get_long("forecastTime")
    tr_ftu = grib.get_long("indicatorOfUnitOfTimeRange")
    if tr_ft is not None and tr_ftu is not None:
        pdtn = grib.get_long("productDefinitionTemplateNumber")
        if pdtn >= 0 and pdtn <= 7:
            md["timerange"] = {
                "style": "Timedef",
                "step_len": tr_ft,
                "step_unit": tr_ftu,
                "stat_type": 254,
                "stat_len": 0,
                "stat_unit": timedef.UNIT_SECOND,
            }
        else:
            tr_sp = grib.get_long("typeOfStatisticalProcessing")
            tr_spu = grib.get_long("indicatorOfUnitForTimeRange")
            tr_spl = grib.get_long("lengthOfTimeRange")
            if tr_sp is not None and tr_spu is not None and tr_spl is not None:
                if pdtn >= 8 and pdtn <= 14 and grib.get_long("typeOfProcessedData") == 0:
                    md["timerange"] = {
                        "style": "Timedef",
                        "step_len": 0,
                        "step_unit": timedef.UNIT_SECOND,
                        "stat_type": tr_sp,
                        "stat_len": tr_spl,
                        "stat_unit": tr_spu,
                    }
                else:
                    val = {
                        "style": "Timedef",
                        "step_len": tr_ft,
                        "step_unit": tr_ftu,
                        "stat_type": tr_sp,
                        "stat_len": tr_spl,
                        "stat_unit": tr_spu,
                    }
                    timedef.make_same_units(val)
                    val["step_len"] += val["stat_len"]
                    md["timerange"] = val
            else:
                if pdtn >= 8 and pdtn <= 14 and grib.get_long("typeOfProcessedData") == 0:
                    md["timerange"] = {
                        "style": "Timedef",
                        "step_len": 0,
                        "step_unit": timedef.UNIT_SECOND,
                    }
                else:
                    md["timerange"] = {
                        "style": "Timedef",
                        "step_len": tr_ft,
                        "step_unit": tr_ftu,
                    }

    # Area
    area = {
        "tn": grib["gridDefinitionTemplateNumber"],
    }
    if area["tn"] == 32768:
        area["utm"] = 1
        area["latfirst"] = grib["northingOfFirstGridPoint"]
        area["lonfirst"] = grib["eastingOfFirstGridPoint"]
        area["latlast"] = grib["northingOfLastGridPoint"]
        area["lonlast"] = grib["eastingOfLastGridPoint"]
        area["fe"] = grib["falseEasting"]
        area["fn"] = grib["falseNorthing"]
        area["zone"] = grib["zone"]
        area["Ni"] = grib["Ni"]
        area["Nj"] = grib["Nj"]
    else:
        if grib["latitudeOfFirstGridPointInDegrees"] is not None:
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
    proddef = {
        "tod": grib.get_long("typeOfProcessedData"),
    }
    if grib.get_long("typeOfProcessedData") >= 3 and grib.get_long("typeOfProcessedData") <= 5:
        proddef["mc"] = grib["marsClass"]
        proddef["mt"] = grib.get_long("marsType")
        proddef["ty"] = grib["typeOfEnsembleForecast"]
        proddef["pf"] = grib["perturbationNumber"]
        proddef["tf"] = grib["numberOfForecastsInEnsemble"]
        if "derivedForecast" in grib:
            proddef["d"] = grib["derivedForecast"]

    if "percentileValue" in grib:
        proddef["pv"] = grib["percentileValue"]

    if "probabilityType" in grib:
        proddef["pt"] = grib.get_long("probabilityType")
        proddef["pl"] = "{},{},{},{}".format(
            grib["scaleFactorOfLowerLimit"],
            grib["scaledValueOfLowerLimit"],
            grib["scaleFactorOfUpperLimit"],
            grib["scaledValueOfUpperLimit"],
        )

    if grib["satelliteSeries"] == 333:
        # Meteosat satellites
        sat_name = METEOSAT_NAMES.get(grib["satelliteNumber"])
        if sat_name is not None:
            proddef["sat"] = sat_name

        if grib["instrumentType"] == 207:
            # Data from the SEVIRI instrument
            ch_name = METEOSAT_CHAN_NAMES.get(
                int(round(10 * 1.0 / (grib["scaledValueOfCentralWaveNumber"] * 0.000001)))
            )
            if ch_name is not None:
                proddef["ch"] = ch_name

    md["proddef"] = {
        "style": "GRIB",
        "value": proddef,
    }


Scanner.register(2, scan_grib2)
