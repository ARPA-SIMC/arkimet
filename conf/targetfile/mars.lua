targetfile["mars"] = function(pattern)
	-- Tags that are expanded in file names
	-- (tag names are uppercase in the table)
	local tags = {}
	tags["DATE"] = function(md)
		local rt = md.reftime
		if rt == nil then return nil end
		local t = rt.time
		if t == nil then return nil end
		return string.format("%04d%02d%02d", t.year, t.month, t.day)
	end
	tags["TIME"] = function(md)
		local rt = md.reftime
		if rt == nil then return nil end
		local t = rt.time
		if t == nil then return nil end
		return string.format("%02d%02d", t.hour, t.minute)
	end
	tags["STEP"] = function(md)
		local tr = md.timerange
		if tr == nil then return nil end
		if tr.style == "GRIB1" then
			if tr.type == nil then return nil end
			return string.format("%02d", tr.p1 / 3600)
		elseif tr.style == "GRIB2" then
			if tr.type == nil then return nil end
			return string.format("%02d", tr.p2)
		else
			return nil
		end
	end

	-- TODO: normalise pattern to remove the need for string.upper later on

	return function(md)
		return string.gsub(pattern, "%b[]", function(tag)
			local func = tags[string.upper(string.sub(tag, 2, -2))]
			if func == nil then return tag end
			local res = func(md)
			if res == nil then return tag end
			return res
		end)
	end
end