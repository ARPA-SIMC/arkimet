-- Format an area: return a string, or nil to fall back to the default
-- formatter
function fmt_area(v)
	if v.style == "GRIB" then
		values = v.val
		-- values is now a table, you can access it for example as
		-- values["lat"] or values.lat
		if not values or not values.type then
			return nil
		end
		drt = read_grib_table(1, "6")
		if not drt then
			return nil
		end
		out= drt:abbr(values.type).." - "..drt:desc(values.type)..": "
	elseif v.style == "VM2" then
		str = "id=" .. tostring(v.id)
		for k, v in pairs(v.dval) do
			str = str .. ", " .. k .. "=" .. tostring(v)
		end
		return str
	end
	return nil
end
