function scan(msg, md)
	area = {}
	lat = msg:find('latitude')
	lon = msg:find('longitude')
	blo = msg:find('block')
	sta = msg:find('station')
	id = msg:find('ident')
    -- Approximate to 1 degree to allow arkimet to perform grouping
    if lat and lon then
        area.latfirst = math.floor(lat:enqd()) * 100000
        area.latlast = math.ceil(lat:enqd()) * 100000
        area.lonfirst = math.floor(lon:enqd()) * 100000
        area.lonlast = math.ceil(lon:enqd()) * 100000
        area.type = 0
    end
	if blo then area.blo = blo:enqi() end
	if sta then area.sta = sta:enqi() end
	if id then area.id = id:enqc() end
	md:set(arki_area.grib(area))
end
