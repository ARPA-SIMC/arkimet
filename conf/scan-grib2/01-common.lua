function scan(md)
	-- Reference time
	local reftime = arki_time.time(grib.year, grib.month, grib.day, grib.hour, grib.minute, grib.second)
	md:set(arki_reftime.position(reftime))

	-- Run
	md:set(arki_run.minute(grib.hour, grib.minute))

	-- Origin
	md:set(arki_origin.grib2(gribl.centre,
	                         gribl.subCentre,
				 gribl.typeOfGeneratingProcess,
				 gribl.backgroundGeneratingProcessIdentifier,
				 gribl.generatingProcessIdentifier))

	-- Product
	md:set(arki_product.grib2(gribl.centre,
				  gribl.discipline,
				  gribl.parameterCategory,
				  gribl.parameterNumber))

	-- Level
	local ltype1, lscale1, lvalue1 = gribl.typeOfFirstFixedSurface, 0, 0
	if gribl.scaleFactorOfFirstFixedSurface ~= -1 then
		lscale1 = gribl.scaleFactorOfFirstFixedSurface
		lvalue1 = gribl.scaledValueOfFirstFixedSurface
	end

	if grib.typeOfSecondFixedSurface and grib.typeOfSecondFixedSurface ~= 255 then
		local ltype2, lscale2, lvalue2 = grib.typeOfSecondFixedSurface, 0, 0
		if gribl.scaleFactorOfSecondFixedSurface ~= -1 then
			lscale2 = grib.scaleFactorOfSecondFixedSurface
			lvalue2 = grib.scaledValueOfSecondFixedSurface
		end
		md:set(arki_level.grib2d(ltype1, lscale1, lvalue1, ltype2, lscale2, lvalue2))
	else
		md:set(arki_level.grib2s(ltype1, lscale1, lvalue1))
	end

	-- Time range
	local ptype, punit, p1, p2 = 0, gribl.stepUnits, 0, gribl.endStep
	if grib.productDefinitionTemplateNumber then
		ptype = grib.productDefinitionTemplateNumber
	end
	if gribl.lengthOfTimeRange ~= nil then
		p1 = gribl.lengthOfTimeRange
	end
	md:set(arki_timerange.grib2(ptype, punit, p1, p2))

	function norm_lon(lon)
		if lon > 180 then
			return lon - 360
		else
			return lon
		end
	end

	-- Area
	local area = {}
	area.tn = grib.gridDefinitionTemplateNumber

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
	md:set(arki_area.grib(area))

	-- Ensemble
	if grib.typeOfProcessedData >= 3 and grib.typeOfProcessedData <= 5 then
		local ensemble = {}
		ensemble.mc = grib.marsClass
		ensemble.mt = gribl.marsType
		ensemble.ty = grib.typeOfEnsembleForecast
		ensemble.pf = grib.perturbationNumber
		ensemble.tf = grib.numberOfForecastsInEnsemble
		if grib.derivedForecast then
			ensemble.d = grib.derivedForecast
		end
		md:set(arki_ensemble.grib(ensemble))
	end
end
