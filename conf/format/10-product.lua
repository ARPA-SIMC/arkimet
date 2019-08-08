-- Format a product: return a string, or nil to fall back to the default
-- formatter
function fmt_product(v)
	if v.style == "GRIB1" then
		origin, product = v.origin, v.product
		if origin == 200 or origin == 80 then
			origin = 0
		end
		names = read_grib_table(1, string.format("2.%d.%d", origin, v.table))
		if names[product] then
			return string.gsub(names:desc(product), '[*][*]', "^")
		end
	elseif v.style == "GRIB2" then
		-- v.centre, v.discipline, v.category, v.number, v.table_version, v.local_table_version
        centre, category, discipline, number, table_version, local_table_version = v.centre, v.category, v.discipline, v.number, v.table_version, v.local_table_version

        tableprefix = get_grib2_table_prefix(centre, table_version, local_table_version)
        tablename = tableprefix .. string.format("4.2.%d.%d", discipline, category)
        names = read_grib_table(2, tablename)

        if names[number] then
            return string.gsub(names:desc(number), '[*][*]', "^")
        end
	elseif v.style == "BUFR" then
		-- v.type, v.subtype, v.localsubtype
	elseif v.style == "VM2" then
		str = "id=" .. tostring(v.id)
		for k, v in pairs(v.dval) do
			str = str .. ", " .. k .. "=" .. tostring(v)
		end
		return str
	end
	return nil
end