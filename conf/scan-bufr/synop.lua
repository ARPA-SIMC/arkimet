function scan(msg, md)
	var = msg:find('block')
	print("Block", var)
	var = msg:find('station')
	print("Station", var)
	lat = msg:find('latitude')
	lon = msg:find('longitude')
	area = arki_area.grib{lat=lat:enqi(), lon=lon:enqi()}
	md:set(area)
	print("ALL OK")
	--print(md)
end
