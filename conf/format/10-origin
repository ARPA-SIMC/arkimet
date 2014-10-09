-- Load code table 0: Identification of centers
grib1_centres = read_grib_table(1, "0")

-- This is how to make a local change to the table
grib1_centres[200] = { "arpa", "ARPA SIM Emilia Romagna" }

-- Format an origin: return a string, or nil to fall back to the default
-- formatter
function fmt_origin(v)
	if v.style == "GRIB1" then
		return string.format("GRIB1 from %s, subcentre %d, process %d",
			grib1_centres:desc(v.centre), v.subcentre, v.process)
	elseif v.style == "GRIB2" then
        -- TODO: index table_version for processtype "{table_version}/4.3"
		-- v.centre, v.subcentre, v.processtype, v.bgprocessid, v.processid
        table_version = 4
        processtypes = read_grib_table(2, string.format("tables/%d/4.3", table_version))
        if processtypes[v.processtype] then
            processtype = processtypes:desc(v.processtype)
        else
            processtype = string.format("type %d", v.processtype)
        end

	    return string.format("GRIB2 from %s, subcentre %d, type %s, background process %d, process %d",
            grib1_centres:desc(v.centre), v.subcentre, processtype, v.bgprocessid, v.processid)
	elseif v.style == "BUFR" then
		-- v.centre, v.subcentre
		return nil
	else
		return nil
	end
end
