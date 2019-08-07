-- Format an origin: return a string, or nil to fall back to the default
-- formatter
function fmt_timerange(v)
	if v.style == "GRIB1" then
		timerange = read_grib_table(1, "5")
		if timerange[v.type] == nil then
			return nil
		end
		str = timerange:desc(v.type) .. " - "
		if v.p1 then str = str .. "p1 " .. v.p1 .. " " end
		if v.p2 then str = str .. "p2 " .. v.p2 .. " " end
		if v.unit then str = str .. "time unit " .. v.unit end
		-- return timerange:desc(v.type).." - p1 "..v.p1..", p2 "..v.p2..", time unit "..v.unit
		return str
    elseif v.style == "Timedef" then
        -- step_unit, step_len, stat_type, stat_unit, stat_len
        step_unit, step_len, stat_type, stat_unit, stat_len = v.step_unit, v.step_len, v.stat_type, v.stat_unit, v.stat_len

        units = {
            [0]="minute",
            [1]="hour",
            [2]="day",
            [3]="month",
            [4]="year",
            [5]="decade",
            [6]="30years",
            [7]="century",
            [10]="3hours",
            [11]="6hours",
            [12]="12hours",
            [13]="second",
            [255]="-",
            abbr=function(self, k)
                local v = self[k]
                if v == nil then
                    return "-"
                else
                    return v
                end
            end
        }

        if stat_type == 254 then
            if step_len == 0 and stat_len == 0 then
                return "Analysis or observation, istantaneous value"
            else
                return string.format("Forecast at t+%d %s, instantaneous value", step_len, units:abbr(step_unit))
            end
        else
            prefixes = {
                [0]="Average",
                [1]="Accumulation",
                [2]="Maximum",
                [3]="Minimum",
                [4]="Difference (end minus beginning)",
                [5]="Root Mean Square",
                [6]="Standard Deviation",
                [7]="Covariance (temporal variance)",
                [8]="Difference (beginning minus end)",
                [9]="Ratio",
                [10]="Standardized anomaly",
                [200]="Vectorial mean",
                [201]="Mode",
                [202]="Standard deviation vectorial mean",
                [203]="Vectorial maximum",
                [204]="Vectorial minimum",
                [205]="Product with a valid time ranging",
                [255]="No statistical processing",
            }
            prefix = prefixes[stat_type]
            if prefix == nil then
                error("Unknown statistical processing " .. stat_type) 
            elseif step_len == nil and stat_len == nil then
                return prefix
            else
                if stat_len ~= nil then
                    prefix = prefix .. string.format(" over %d %s", stat_len, units:abbr(stat_unit))
                end
                if step_len == nil then
                    return prefix
                elseif step_len < 0 then
                    return prefix .. string.format(" %d %s before reference time", (-1 * step_len), units:abbr(step_unit))
                else
                    return prefix .. string.format(" at forecast time %d %s", step_len, units:abbr(step_unit))
                end
            end
        end
	end
	return nil
end
