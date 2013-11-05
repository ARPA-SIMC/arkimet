function find_mandatory_attr(grp, attr)
	local v = odimh5:find_attr(grp, attr) or error(grp .. "/@" .. attr .. " not found")
	return v
end

function odimh5_datetime2sql(date, time)
	return date:sub(1,4) .. "-" .. date:sub(5,6) .. "-" .. date:sub(7,8) .. " " .. time:sub(1,2) .. ":" .. time:sub(3,4) .. ":" .. time:sub(5,6)
end

function odimh5_datetime2epoch(date, time)
	local t = {}
	t["year"] = tonumber(date:sub(1,4))
	t["month"] = tonumber(date:sub(5,6))
	t["day"] = tonumber(date:sub(7,8))
	t["hour"] = tonumber(time:sub(1,2))
	t["min"] = tonumber(time:sub(3,4))
	t["sec"] = tonumber(time:sub(5,6))
	t["tz"] = "GMT"
	return os.time(t)
end

function odimh5_set_reftime(md)
	local date = find_mandatory_attr("/what", "date")
	local time = find_mandatory_attr("/what", "time")
	md:set(arki_reftime.position(arki_time.sql(odimh5_datetime2sql(date, time))))
end

function odimh5_set_origin(md)
	local source = find_mandatory_attr("/what", "source")

	local wmo, n = source:gsub(".*WMO:([^,]+).*", "%1")
	if n == 0 then wmo = "" end
	local rad, n = source:gsub(".*RAD:([^,]+).*", "%1")
	if n == 0 then rad = "" end
	local plc, n = source:gsub(".*PLC:([^,]+).*", "%1")
	if n == 0 then plc = "" end

	md:set(arki_origin.odimh5(wmo, rad, plc))
end

function odimh5_pvol_set_level(md)
	local min = 360
	local max = -360
	for ds, _ in pairs(odimh5:get_groups("/")) do
		if ds:match("^dataset") then
			local elangle = odimh5:find_attr(ds .. "/where", "elangle")
			if elangle then
				if elangle > max then max = elangle end
				if elangle < min then min = elangle end
			end
		end
	end
	md:set(arki_level.odimh5(min, max))
end

function odimh5_set_quantity(md)
	local quantities = {}
	for ds, _ in pairs(odimh5:get_groups("/")) do
		if ds:match("^dataset") then
			for data, _ in pairs(odimh5:get_groups("/" .. ds)) do
				local quantity = odimh5:find_attr(ds .. "/" .. data .. "/what", "quantity")
				if quantity ~= nil then table.insert(quantities, quantity) end
			end
		end
	end
	md:set(arki_quantity.new(quantities))
end

function odimh5_pvol_set_area(md)
	local lat = find_mandatory_attr("/where", "lat")
	local lon = find_mandatory_attr("/where", "lon")
	local radius = 0
	for ds, _ in pairs(odimh5:get_groups("/")) do
		if ds:match("^dataset") then
			local radhor = odimh5:find_attr(ds .. "/how", "radhoriz")
			if radhor ~= nil and radhor > radius then
				radius = radhor
			end
		end
	end
	md:set(arki_area.odimh5{lat=lat*1000000,lon=lon*1000000,radius=radius*1000})
end

function odimh5_horizobj_set_area(md)
	local lons = {
		find_mandatory_attr("/where", "LL_lon"),
		find_mandatory_attr("/where", "LR_lon"),
		find_mandatory_attr("/where", "UL_lon"),
		find_mandatory_attr("/where", "UR_lon"),
	}
	local lats = {
		find_mandatory_attr("/where", "LL_lat"),
		find_mandatory_attr("/where", "LR_lat"),
		find_mandatory_attr("/where", "UL_lat"),
		find_mandatory_attr("/where", "UR_lat"),
	}
	md:set(arki_area.grib{
		type=0,
		latfirst=math.min(unpack(lats))*1000000,
		latlast=math.max(unpack(lats))*1000000,
		lonfirst=math.min(unpack(lons))*1000000,
		lonlast=math.max(unpack(lons))*1000000,
	})
end

function odimh5_xsec_set_area(md)
	local lons = {
		find_mandatory_attr("/where", "start_lon"),
		find_mandatory_attr("/where", "stop_lon"),
	}
	local lats = {
		find_mandatory_attr("/where", "start_lat"),
		find_mandatory_attr("/where", "stop_lat"),
	}
	md:set(arki_area.grib{
		type=0,
		latfirst=find_mandatory_attr("/where", "start_lat")*1000000,
		latlast=find_mandatory_attr("/where", "stop_lat")*1000000,
		lonfirst=find_mandatory_attr("/where", "start_lon")*1000000,
		lonlast=find_mandatory_attr("/where", "stop_lon")*1000000,
	})
end

function scan(md)
	local conventions = find_mandatory_attr("/", "Conventions")
	-- reftime
	odimh5_set_reftime(md)
	-- origin
	odimh5_set_origin(md)

	local object = find_mandatory_attr("/what", "object")
	-- polar volume
	if object == "PVOL" then
		-- product
		md:set(arki_product.odimh5("PVOL","SCAN"))
		-- task
		local task = find_mandatory_attr("/how", "task")
		md:set(arki_task.new(task))
		-- level
		odimh5_pvol_set_level(md)
		-- quantity
		odimh5_set_quantity(md)
		-- area
		odimh5_pvol_set_area(md)
	elseif object == "COMP" or object == "IMAGE" then
		-- product
		local product = find_mandatory_attr("/dataset1/what", "product")
		md:set(arki_product.odimh5(object, product))
		-- task
		local task = find_mandatory_attr("/how", "task")
		md:set(arki_task.new(task))
		-- quantity
		odimh5_set_quantity(md)
		-- area
		odimh5_horizobj_set_area(md)
		if product == "CAPPI" or product == "PCAPPI" then
			local height = find_mandatory_attr("/dataset1/what", "prodpar")
			md:set(arki_level.grib1(105, height))
		elseif product == "PPI" then
			local height = find_mandatory_attr("/dataset1/what", "prodpar")
			md:set(arki_level.odimh5(height))
		elseif product == "RR" then
			local date1 = find_mandatory_attr("/dataset1/what", "startdate")
			local time1 = find_mandatory_attr("/dataset1/what", "starttime")
			local date2 = find_mandatory_attr("/dataset1/what", "enddate")
			local time2 = find_mandatory_attr("/dataset1/what", "endtime")
			local diff = odimh5_datetime2epoch(date2, time2) - odimh5_datetime2epoch(date1, time1)
			md:set(arki_timerange.timedef("0s", 1, diff .. "s"))
		elseif product == "VIL" then
			local prodpar = find_mandatory_attr("/dataset1/what", "prodpar")
			local bottom, top = prodpar:match("(%d+),(%d+)")
			md:set(arki_level.grib1(106, top/100, bottom/100))
		elseif odimh5:find_attr("/dataset2/what", "product") and odimh5:find_attr("/dataset3/what", "product") then
			md:set(arki_product.odimh5(object, "HVMI"))
		end
	elseif object == "XSEC" then
		-- product
		local product = find_mandatory_attr("/dataset1/what", "product")
		md:set(arki_product.odimh5(object, product))
		-- task
		local task = find_mandatory_attr("/how", "task")
		md:set(arki_task.new(task))
		-- quantity
		odimh5_set_quantity(md)
		-- area
		odimh5_xsec_set_area(md)
	end
end
