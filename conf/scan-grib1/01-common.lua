local cosmo_centres = { [78]=1, [80]=1, [200]=1 }
local cosmo_nudging_table2 = {
     [11]=0,  [15]=2,  [16]=3,  [17]=0,  [33]=0,  [34]=0,
     [57]=1,  [61]=1,  [78]=1,  [79]=1,  [90]=1, [111]=0,
    [112]=0, [113]=0, [114]=0, [121]=0, [122]=0, [124]=0,
    [125]=0, [126]=0, [127]=0 }
local cosmo_nudging_table201 = {
      [5]=0,  [20]=1,  [22]=0,  [23]=0,  [24]=0,  [25]=0,
     [26]=0,  [27]=0,  [42]=1, [102]=1, [113]=1, [132]=1,
    [135]=1, [187]=2, [218]=2, [219]=2 }
local cosmo_nudging_table202 = {
    [231]=0, [232]=0, [233]=0 }

function scan(md)
	-- Reference time
	local year = (grib.centuryOfReferenceTimeOfData - 1) * 100 + grib.yearOfCentury
	local reftime = arki_time.time(year, grib.month, grib.day, grib.hour, grib.minute, grib.second)
	md:set(arki_reftime.position(reftime))

	-- Run
	md:set(arki_run.minute(grib.hour, grib.minute))

	-- Origin
	md:set(arki_origin.grib1(gribl.centre, gribl.subCentre, gribl.generatingProcessIdentifier))

	-- Product
	md:set(arki_product.grib1(gribl.centre, gribl.gribTablesVersionNo, gribl.indicatorOfParameter))

	-- Level
	local ltype = gribl.indicatorOfTypeOfLevel
	if grib.levels == nil
	then
		md:set(arki_level.grib1(ltype, grib.level, 0))
	else
		md:set(arki_level.grib1(ltype, grib.topLevel, grib.bottomLevel))
	end

	-- Time range
    if gribl.timeRangeIndicator == 13 and cosmo_centres[gribl.centre]
    then
        -- COSMO 'nudging'
        -- Special scan directly to timedef timeranges
        -- needs to be done here since it depends on the parameter
        local statproc = nil
        if grib.P2 ~= 0 then
            -- guess timerange according to parameter
            if gribl.gribTablesVersionNo == 2 then
                -- table 2
                statproc = cosmo_nudging_table2[gribl.indicatorOfParameter]
            elseif gribl.gribTablesVersionNo == 201 then
                -- table 201
                statproc = cosmo_nudging_table201[gribl.indicatorOfParameter]
            elseif gribl.gribTablesVersionNo == 202 then
                -- table 202
                statproc = cosmo_nudging_table202[gribl.indicatorOfParameter]
            end
        end
        if statproc == nil then
            -- default to point in time
            md:set(arki_timerange.timedef(0, "s", 254))
        else
            -- known statistical processing
            local tunit = gribl.indicatorOfUnitOfTimeRange
            if tunit == 254 then tunit = 13 end
            md:set(arki_timerange.timedef(0, "s", statproc, grib.P2-grib.P1, tunit))
        end
    else
        md:set(arki_timerange.grib1(grib.timeRangeIndicator, grib.indicatorOfUnitOfTimeRange, grib.P1, grib.P2))
    end

	-- Area
	local area = {}
	area.type = grib.dataRepresentationType

	if grib.latitudeOfFirstGridPointInDegrees ~= nil then
		if grib.latitudeOfFirstGridPointInDegrees >= 1000 then
			-- UTM hack
			area.utm = 1

			area.latfirst = grib.latitudeOfFirstGridPointInDegrees * 1000
			area.lonfirst = grib.longitudeOfFirstGridPointInDegrees * 1000

			if grib.numberOfPointsAlongAParallel then
				area.Ni = grib.numberOfPointsAlongAParallel
				area.Nj = grib.numberOfPointsAlongAMeridian
				area.latlast = grib.latitudeOfLastGridPointInDegrees * 1000
				area.lonlast = grib.longitudeOfLastGridPointInDegrees * 1000
			end
			if grib.numberOfPointsAlongXAxis then
				area.Ni = grib.numberOfPointsAlongXAxis
				area.Nj = grib.numberOfPointsAlongYAxis
			end
		else
			area.latfirst = grib.latitudeOfFirstGridPointInDegrees * 1000000
			area.lonfirst = grib.longitudeOfFirstGridPointInDegrees * 1000000

			if grib.numberOfPointsAlongAParallel then
				area.Ni = grib.numberOfPointsAlongAParallel
				area.Nj = grib.numberOfPointsAlongAMeridian
				area.latlast = grib.latitudeOfLastGridPointInDegrees * 1000000
				area.lonlast = grib.longitudeOfLastGridPointInDegrees * 1000000
			end
			if grib.numberOfPointsAlongXAxis then
				area.Ni = grib.numberOfPointsAlongXAxis
				area.Nj = grib.numberOfPointsAlongYAxis
			end
			if grib.angleOfRotationInDegrees then
				area.rot = grib.angleOfRotationInDegrees * 1000000
				area.latp = grib.latitudeOfSouthernPoleInDegrees * 1000000
				area.lonp = grib.longitudeOfSouthernPoleInDegrees * 1000000
			end
		end
	end
	md:set(arki_area.grib(area))

	-- Proddef
	local proddef = {}
	if grib.localDefinitionNumber then
		proddef.ld = gribl.localDefinitionNumber
		proddef.mt = gribl.marsType
		if grib.clusterNumber then
			proddef.nn = gribl.clusterNumber
		end
		if grib.forecastProbabilityNumber then
			proddef.nn = gribl.forecastProbabilityNumber
		end
		if grib.perturbationNumber then
			proddef.nn = gribl.perturbationNumber
		end
		md:set(arki_proddef.grib(proddef))
	end
end
