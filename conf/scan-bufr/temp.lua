function scan(msg, md)
	blo = msg:find('block')
	sta = msg:find('station')
	lat = msg:find('latitude')
	lon = msg:find('longitude')
	area = arki_area.grib{lat=lat:enqi(), lon=lon:enqi(), blo=blo:enqi(), sta=sta:enqi()}
	md:set(area)

	forecast = nil
	msg:foreach(function(ctx)
		if ctx:pind() == 1 then
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
	md:set(arki_timerange.bufr(forecast))
end
