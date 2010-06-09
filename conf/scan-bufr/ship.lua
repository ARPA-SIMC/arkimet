function scan(msg, md)
	area = {}
	lat = msg:find('latitude')
	lon = msg:find('longitude')
	blo = msg:find('block')
	sta = msg:find('station')
	id = msg:find('ident')
	if lat then area.lat = lat:enqi() end
	if lon then area.lon = lon:enqi() end
	if blo then area.blo = blo:enqi() end
	if sta then area.sta = sta:enqi() end
	if id then area.id = id:enqc() end
	md:set(arki_area.grib(area))
end
