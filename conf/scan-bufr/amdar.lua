function scan(msg, md)
    local area = bufr_read_area_mobile(msg)
    local proddef = bufr_read_proddef(msg)
    if area then md:set(arki_area.grib(area)) end
    if proddef then md:set(arki_proddef.grib(proddef)) end
end
