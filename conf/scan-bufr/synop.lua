function scan(msg, md)
	blo = msg:find('block')
	sta = msg:find('station')
	lat = msg:find('latitude')
	lon = msg:find('longitude')
	area = arki_area.grib{lat=lat:enqi(), lon=lon:enqi(), blo=blo:enqi(), sta=sta:enqi()}
	md:set(area)
end
