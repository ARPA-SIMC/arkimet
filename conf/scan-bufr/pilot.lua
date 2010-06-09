function scan(msg, md)
	area = {}
	lat = msg:find('latitude')
	lon = msg:find('longitude')
	blo = msg:find('block')
	sta = msg:find('station')
	id = msg:find('ident')
	if lat then area.lat = sta:enqi() end
	if lon then area.lon = sta:enqi() end
	if blo then area.blo = blo:enqi() end
	if sta then area.sta = sta:enqi() end
	if id then area.id = id:enqc() end
	md:set(arki_area.grib(area))

	forecast = nil
	msg:foreach(function(ctx)
		if ctx:pind() == 254 then
			-- TODO: verify that all p1 are the same
			-- if they differ, error()
			p1 = ctx:p1()
			if forecast ~= nil and forecast ~= p1 then
				error("BUFR message has contradictory forecasts"..tostring(forecast).." and "..tostring(p1))
			end
			forecast = p1
		end
	end)
	if forecast == nil then forecast = 0 end

	-- TODO: If p1 != 0 then reftime = reftime - p1

	if forecast == 0 then
		md:set(arki_timerange.bufr(forecast))
	elseif math.floor(forecast / 3600) * 3600 == forecast then
		-- If it is a multiple of one hour, represent it in hours
		md:set(arki_timerange.bufr(forecast / 3600, 1))
	else
		md:set(arki_timerange.bufr(forecast, 254))
	end
end
