function bufr_read_area_fixed(msg)
    local lat = msg:find('latitude')
    local lon = msg:find('longitude')
    if lat ~= nil and lon ~= nil then
        return { lat=lat:enqi(), lon=lon:enqi() }
    else
        return nil
    end
end

function bufr_read_area_mobile(msg)
    local lat = msg:find('latitude')
    local lon = msg:find('longitude')
    if lat ~= nil and lon ~= nil then
        -- Approximate to 1 degree to allow arkimet to perform grouping
        return {
            type="mob",
            x=math.floor(lon:enqd()),
            y=math.floor(lat:enqd())
        }
    else
        return nil
    end
end

function bufr_read_proddef(msg)
    local res = {}
    local blo = msg:find('block')
    local sta = msg:find('station')
    local id = msg:find('ident')
    local found = false
    if blo then
        res.blo = blo:enqi()
        found = true
    end
    if sta then
        res.sta = sta:enqi()
        found = true
    end
    if id then
        res.id = id:enqc()
        found = true
    end
    if found then
        return res
    else
        return nil
    end
end

function bufr_scan_forecast(msg)
    local forecast = nil
    msg:foreach(function(ctx)
        if ctx:pind() == 254 then
            -- Verify that all p1 are the same: if they differ, error()
            local p1 = ctx:p1()
            if forecast ~= nil and forecast ~= p1 then
                error("BUFR message has contradictory forecasts"..tostring(forecast).." and "..tostring(p1))
            end
            forecast = p1
        end
    end)
    if forecast == nil then forecast = 0 end

    if forecast == 0 then
        return arki_timerange.timedef(0)
    elseif math.floor(forecast / 3600) * 3600 == forecast then
        -- If it is a multiple of one hour, represent it in hours
        return arki_timerange.timedef(forecast / 3600, "h")
    elseif math.floor(forecast / 60) * 60 == forecast then
        -- If it is a multiple of one minute, represent it in minutes
        return arki_timerange.timedef(forecast / 60, "m")
    else
        return arki_timerange.timedef(forecast, "s")
    end
end
