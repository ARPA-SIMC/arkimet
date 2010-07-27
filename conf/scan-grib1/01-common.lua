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
	md:set(arki_timerange.grib1(grib.timeRangeIndicator, grib.indicatorOfUnitOfTimeRange, grib.P1, grib.P2))

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

	-- Ensemble
	local ensemble = {}
	if grib.localDefinitionNumber then
		ensemble.ld = grib.localDefinitionNumber
		ensemble.mt = grib.marsType
		if grib.clusterNumber then
			ensemble.nn = grib.clusterNumber
		end
		if grib.forecastProbabilityNumber then
			ensemble.nn = grib.forecastProbabilityNumber
		end
		if grib.perturbationNumber then
			ensemble.nn = grib.perturbationNumber
		end
		md:set(arki_ensemble.grib(ensemble))
	end
end
