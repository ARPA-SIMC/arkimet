function scan(msg, md)
    lat = msg:find('latitude')
    lon = msg:find('longitude')
    blo = msg:find('block')
    sta = msg:find('station')
    id = msg:find('ident')
    -- Approximate to 1 degree to allow arkimet to perform grouping
    if lat and lon then
        area = {}
        area.type = "mob"
        area.x = math.floor(lon:enqd())
        area.y = math.floor(lat:enqd())
        md:set(arki_area.grib(area))
    end
    if blo then area.blo = blo:enqi() end
    if sta then area.sta = sta:enqi() end
    if id then md:set(arki_proddef.grib{id=id:enqc()}) end
end
