local pollution_codes = {
	B15192 = "NO",
	B15193 = "NO2",
	B15194 = "O3",
	B15195 = "PM10",
	B15196 = "CO",
	B15197 = "SO2",
	B15198 = "PM2.5",
}

function scan(msg, md)
	area = {}
	lat = msg:find('latitude')
	lon = msg:find('longitude')
	blo = msg:find('block')
	sta = msg:find('station')
	id = msg:find('ident')
	gems = msg:find('B01214', 257, nil, nil, nil, nil, nil, nil)
	name = msg:find('B01019', 257, nil, nil, nil, nil, nil, nil)

	-- Look for pollutant type
	polltype = nil
	msg:foreach(function(ctx)
		if ctx:pind() ~= 257 and polltype == nil then
			ctx:foreach(function(var)
				if polltype == nil then
					polltype = pollution_codes[var:code()]
				end
			end)
		end
	end)

	if lat then area.lat = lat:enqi() end
	if lon then area.lon = lon:enqi() end
	if blo then area.blo = blo:enqi() end
	if sta then area.sta = sta:enqi() end
	if id then area.id = id:enqc() end
	if gems then area.gems = gems:enqc() end
	if name then area.name = name:enqc() end
	md:set(arki_area.grib(area))

	if polltype ~= nil then md:set(md.product:addValues{p=polltype}) end
end
