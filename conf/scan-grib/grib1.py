from arkimet.scan.grib import Scanner

cosmo_centres = {
    78: 1, 80: 1, 200: 1
}
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
# 	local ltype = gribl.indicatorOfTypeOfLevel
# 	if grib.levels == nil
# 	then
# 		md:set(arki_level.grib1(ltype, grib.level, 0))
# 	else
# 		md:set(arki_level.grib1(ltype, grib.topLevel, grib.bottomLevel))
# 	end

#     -- Create this array here so time range can add to it
#     local proddef = {}
# 
    # Time range
#     if gribl.timeRangeIndicator == 13 and cosmo_centres[gribl.centre]
#     then
#         -- COSMO 'nudging'
#         -- Special scan directly to timedef timeranges
#         -- needs to be done here since it depends on the parameter
# 
#         proddef.tod = 0 -- Analysis product
# 
#         -- Instantaneous data
#         if grib.P1 == 0 and grib.P2 == 0 then
#             md:set(arki_timerange.timedef(0, "s", 254, 0, "s"))
#         elseif grib.P1 > grib.P2 then
#             -- TODO: this could be a general warning
#             error("COSMO message with P1 > P2")
#         else
#             local statproc = nil
# 
#             -- guess timerange according to parameter
#             if gribl.gribTablesVersionNo == 2 then
#                 -- table 2
#                 statproc = cosmo_nudging_table2[gribl.indicatorOfParameter]
#             elseif gribl.gribTablesVersionNo == 201 then
#                 -- table 201
#                 statproc = cosmo_nudging_table201[gribl.indicatorOfParameter]
#             elseif gribl.gribTablesVersionNo == 202 then
#                 -- table 202
#                 statproc = cosmo_nudging_table202[gribl.indicatorOfParameter]
#             end
#             -- If gribTablesVersionNo is unsupported or if
#             -- gribl.indicatorOfParameter is not found in tables, statproc
#             -- is nil and later below we default to 'point in time' and
#             -- 'cosmo nudging analysis'
# 
#             if statproc == nil then
#                 -- default to point in time, with 'cosmo nudging analysis' as
#                 -- statistical processing
#                 error("Unknown COSMO parameter type")
#             else
#                 -- known statistical processing
#                 local tunit = gribl.indicatorOfUnitOfTimeRange
#                 if tunit == 254 then tunit = 13 end
#                 md:set(arki_timerange.timedef(0, "s", statproc, grib.P2-grib.P1, tunit))
#             end
#         end
#     else
#         md:set(arki_timerange.grib1(grib.timeRangeIndicator, grib.indicatorOfUnitOfTimeRange, grib.P1, grib.P2))
#         proddef.tod = 1 -- Initial instant of forecast
#     end

    # Area
# 	local area = {}
# 	area.type = grib.dataRepresentationType
# 
# 	if grib.latitudeOfFirstGridPointInDegrees ~= nil then
# 		if grib.latitudeOfFirstGridPointInDegrees >= 1000 then
# 			-- UTM hack
# 			area.utm = 1
# 
# 			area.latfirst = grib.latitudeOfFirstGridPointInDegrees * 1000
# 			area.lonfirst = grib.longitudeOfFirstGridPointInDegrees * 1000
# 
# 			if grib.numberOfPointsAlongAParallel then
# 				area.Ni = grib.numberOfPointsAlongAParallel
# 				area.Nj = grib.numberOfPointsAlongAMeridian
# 				area.latlast = grib.latitudeOfLastGridPointInDegrees * 1000
# 				area.lonlast = grib.longitudeOfLastGridPointInDegrees * 1000
# 			end
# 			if grib.numberOfPointsAlongXAxis then
# 				area.Ni = grib.numberOfPointsAlongXAxis
# 				area.Nj = grib.numberOfPointsAlongYAxis
# 			end
# 		else
# 			area.latfirst = grib.latitudeOfFirstGridPointInDegrees * 1000000
# 			area.lonfirst = grib.longitudeOfFirstGridPointInDegrees * 1000000
# 
# 			if grib.numberOfPointsAlongAParallel then
# 				area.Ni = grib.numberOfPointsAlongAParallel
# 				area.Nj = grib.numberOfPointsAlongAMeridian
# 				area.latlast = grib.latitudeOfLastGridPointInDegrees * 1000000
# 				area.lonlast = grib.longitudeOfLastGridPointInDegrees * 1000000
# 			end
# 			if grib.numberOfPointsAlongXAxis then
# 				area.Ni = grib.numberOfPointsAlongXAxis
# 				area.Nj = grib.numberOfPointsAlongYAxis
# 			end
# 			if grib.angleOfRotationInDegrees then
# 				area.rot = grib.angleOfRotationInDegrees * 1000000
# 				area.latp = grib.latitudeOfSouthernPoleInDegrees * 1000000
# 				area.lonp = grib.longitudeOfSouthernPoleInDegrees * 1000000
# 			end
# 		end
# 	end
# 	md:set(arki_area.grib(area))
# 
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
