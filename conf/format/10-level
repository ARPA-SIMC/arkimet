-- Format a level: return a string, or nil to fall back to the default
-- formatter
function fmt_level(v)
	if v.style == "GRIB1" then
		type, l1, l2 = v.type, v.l1, v.l2
		levels = read_grib_table(1, "3")
		if levels[type] then
			if l1 == nil then
				l1 = "-"
			end
			if l2 == nil then
				l2 = "-"
			end
			return string.format("%s %s %s %s", 
					     levels:abbr(type),
					     levels:desc(type),
					     l1, l2)
		end
	elseif v.style == "GRIB2S" or v.style == "GRIB2D" then
        -- TODO: index centre, table_version, local_table_version
        centre, table_version, local_table_version = nil, nil, nil

        if v.style == "GRIB2S" then
            type1, scale1, value1 = v.type, v.scale, v.value
            type2, scale2, value2 = nil, nil, nil
        else
            type1, scale1, value1 = v.type1, v.scale1, v.value1
            type2, scale2, value2 = v.type2, v.scale2, v.value2
        end

        levels = read_grib_table(2, get_grib2_table_prefix(centre, table_version, local_table_version) .. "4.5")

        function format_single_level(type, scale, value)
            if levels[type] ~= nil then
                res =  string.format("%s %s", levels:abbr(type), levels:desc(type))
                if value == nil then
                    value = "-"
                end
                if scale == nil then
                    scale = "-"
                end
                return string.format("%s %s %s %s", levels:abbr(type), levels:desc(type), scale, value)
            else
                return nil
            end
        end

        if type2 == nil then
            return format_single_level(type1, scale1, value1)
        else
            return format_single_level(type1, scale1, value1) .. " " .. format_single_level(type2, scale2, value2)
        end
    end

	return nil
end
