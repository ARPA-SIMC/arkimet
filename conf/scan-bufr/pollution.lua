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
    local area = bufr_read_area_fixed(msg)
    if area then md:set(arki_area.grib(area)) end

    local proddef = bufr_read_proddef(msg)
    gems = msg:find('B01214', 257, nil, nil, nil, nil, nil, nil)
    name = msg:find('B01019', 257, nil, nil, nil, nil, nil, nil)
    if gems or name then
        if proddef == nil then proddef = {} end
        if gems then proddef.gems = gems:enqc() end
        if name then proddef.name = name:enqc() end
    end
    if proddef then md:set(arki_proddef.grib(proddef)) end

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
    if polltype ~= nil then md:set(md.product:addValues{p=polltype}) end
end
