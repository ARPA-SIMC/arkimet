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
    local tr_ft = gribl.forecastTime
    local tr_ftu = gribl.indicatorOfUnitOfTimeRange
    if tr_ft ~= nil and tr_ftu ~= nil then
        local pdtn = gribl.productDefinitionTemplateNumber
        if pdtn >= 0 and pdtn <= 7 then
            md:set(arki_timerange.timedef(tr_ft, tr_ftu, 254))
        else
            local tr_sp = gribl.typeOfStatisticalProcessing
            local tr_spu = gribl.indicatorOfUnitForTimeRange
            local tr_spl = gribl.lengthOfTimeRange
            if tr_sp ~= nil and tr_spu ~= nil and tr_spl ~= nil then
                md:set(arki_timerange.timedef(tr_ft, tr_ftu, tr_sp, tr_spl, tr_spu))
            else
                md:set(arki_timerange.timedef(tr_ft, tr_ftu))
            end
        end
    end

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
    if area.tn == 32768 then
        area.utm = 1
        area.latfirst = grib.northingOfFirstGridPoint
        area.lonfirst = grib.eastingOfFirstGridPoint
        area.latlast = grib.northingOfLastGridPoint
        area.lonlast = grib.eastingOfLastGridPoint
        area.fe = grib.falseEasting
        area.fn = grib.falseNorthing
        area.zone = grib.zone
	area.Ni = grib.Ni
	area.Nj = grib.Nj
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
	md:set(arki_area.grib(area))

	-- Proddef
	if gribl.typeOfProcessedData >= 3 and gribl.typeOfProcessedData <= 5 then
		local proddef = {}
		proddef.mc = grib.marsClass
		proddef.mt = gribl.marsType
		proddef.ty = grib.typeOfEnsembleForecast
		proddef.pf = grib.perturbationNumber
		proddef.tf = grib.numberOfForecastsInEnsemble
		if grib.derivedForecast then
			proddef.d = grib.derivedForecast
		end
		md:set(arki_proddef.grib(proddef))
	end
end
