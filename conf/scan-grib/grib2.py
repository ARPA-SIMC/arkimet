from arkimet.scan.grib import Scanner


# 	function norm_lon(lon)
# 		if lon > 180 then
# 			return lon - 360
# 		else
# 			return lon
# 		end
# 	end

def scan_grib2(grib, md):
    # Reference time
#	local reftime = arki_time.time(grib.year, grib.month, grib.day, grib.hour, grib.minute, grib.second)
#	md:set(arki_reftime.position(reftime))

    # Run
#	md:set(arki_run.minute(grib.hour, grib.minute))

    # Origin
    md["origin"] = {
        "style": "GRIB2",
        "centre": grib.get_long("centre"),
        "subcentre": grib.get_long("subCentre"),
        "process_type": grib.get_long("typeOfGeneratingProcess"),
        "background_process_id": grib.get_long("backgroundGeneratingProcessIdentifier"),
        "process_id": grib.get_long("generatingProcessIdentifier"),
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
        lscale1 = grib.get_long("scaleFactorOfFirstFixedSurface)")
        lvalue1 = grib.get_long("scaledValueOfFirstFixedSurface)")

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
#    local tr_ft = gribl.forecastTime
#    local tr_ftu = gribl.indicatorOfUnitOfTimeRange
#    if tr_ft ~= nil and tr_ftu ~= nil then
#        local pdtn = gribl.productDefinitionTemplateNumber
#        if pdtn >= 0 and pdtn <= 7 then
#            md:set(arki_timerange.timedef(tr_ft, tr_ftu, 254, 0, "s"))
#        else
#            local tr_sp = gribl.typeOfStatisticalProcessing
#            local tr_spu = gribl.indicatorOfUnitForTimeRange
#            local tr_spl = gribl.lengthOfTimeRange
#            if tr_sp ~= nil and tr_spu ~= nil and tr_spl ~= nil then
#                if pdtn >= 8 and pdtn <= 14 and gribl.typeOfProcessedData == 0 then
#                    md:set(arki_timerange.timedef(0, "s", tr_sp, tr_spl, tr_spu))
#                else
#                    md:set(arki_timerange.timedef_combined(tr_ft, tr_ftu, tr_sp, tr_spl, tr_spu))
#                end
#            else
#                if pdtn >= 8 and pdtn <= 14 and gribl.typeOfProcessedData == 0 then
#                    md:set(arki_timerange.timedef(0, "s"))
#                else
#                    md:set(arki_timerange.timedef(tr_ft, tr_ftu))
#                end
#            end
#        end
#    end

    # Area
#	local area = {}
#	area.tn = grib.gridDefinitionTemplateNumber
#    if area.tn == 32768 then
#        area.utm = 1
#        area.latfirst = grib.northingOfFirstGridPoint
#        area.lonfirst = grib.eastingOfFirstGridPoint
#        area.latlast = grib.northingOfLastGridPoint
#        area.lonlast = grib.eastingOfLastGridPoint
#        area.fe = grib.falseEasting
#        area.fn = grib.falseNorthing
#        area.zone = grib.zone
#	area.Ni = grib.Ni
#	area.Nj = grib.Nj
#    else
#      if grib.latitudeOfFirstGridPointInDegrees ~= nil then
#        area.latfirst = grib.latitudeOfFirstGridPointInDegrees * 1000000
#        area.lonfirst = grib.longitudeOfFirstGridPointInDegrees * 1000000
#
#        if grib.numberOfPointsAlongAParallel then
#            area.Ni = grib.numberOfPointsAlongAParallel
#            area.Nj = grib.numberOfPointsAlongAMeridian
#            area.latlast = grib.latitudeOfLastGridPointInDegrees * 1000000
#            area.lonlast = grib.longitudeOfLastGridPointInDegrees * 1000000
#        end
#        if grib.numberOfPointsAlongXAxis then
#            area.Ni = grib.numberOfPointsAlongXAxis
#            area.Nj = grib.numberOfPointsAlongYAxis
#        end
#        if grib.angleOfRotationInDegrees then
#            area.rot = grib.angleOfRotationInDegrees * 1000000
#            area.latp = grib.latitudeOfSouthernPoleInDegrees * 1000000
#            area.lonp = grib.longitudeOfSouthernPoleInDegrees * 1000000
#        end
#      end
#    end
#	md:set(arki_area.grib(area))

    # Proddef
#    local proddef = {}
#    proddef.tod = gribl.typeOfProcessedData
#	if gribl.typeOfProcessedData >= 3 and gribl.typeOfProcessedData <= 5 then
#		proddef.mc = grib.marsClass
#		proddef.mt = gribl.marsType
#		proddef.ty = grib.typeOfEnsembleForecast
#		proddef.pf = grib.perturbationNumber
#		proddef.tf = grib.numberOfForecastsInEnsemble
#		if grib.derivedForecast then
#			proddef.d = grib.derivedForecast
#		end
#	end
#    md:set(arki_proddef.grib(proddef))


Scanner.register(2, scan_grib2)
