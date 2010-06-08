function scan(msg, md)
	blo = msg:find('block')
	sta = msg:find('station')
	lat = msg:find('latitude')
	lon = msg:find('longitude')
	area = arki_area.grib{lat=lat:enqi(), lon=lon:enqi(), blo=blo:enqi(), sta=sta:enqi()}
	md:set(area)

	ptype, p1, p2 = 254, 0, 0
	msg:foreach(function(ctx)
		if ctx:pind() == 1 then
			ptype = ctx:pind()
			p1 = ctx:p1()
			p2 = ctx:p2()
		end
	end)
	md:set(arki_timerange.grib1(ptype, 254, p1, p2))
end
