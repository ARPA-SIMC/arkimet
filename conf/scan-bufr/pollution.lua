function scan(msg, md)
	area = {}
	lat = msg:find('latitude')
	lon = msg:find('longitude')
	blo = msg:find('block')
	sta = msg:find('station')
	id = msg:find('ident')
	gems = msg:find('B01214', 257, 0, 0, 0, 0, 0, 0)
	name = msg:find('B01019', 257, 0, 0, 0, 0, 0, 0)

	if lat then area.lat = lat:enqi() end
	if lon then area.lon = lon:enqi() end
	if blo then area.blo = blo:enqi() end
	if sta then area.sta = sta:enqi() end
	if id then area.id = id:enqc() end
	if gems then area.gems = gems:enqc() end
	if name then area.name = name:enqc() end
	md:set(arki_area.grib(area))
end
