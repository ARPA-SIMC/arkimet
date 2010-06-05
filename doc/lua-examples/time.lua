--- Examples on how to use time objects from Lua

-- GRIB1 time

-- Create with year...second components
local o = arki_time.time(1789, 7, 14, 12, 0, 0)

-- Accessors by name
ensure_equals(o.year, 1789)
ensure_equals(o.month, 7)
ensure_equals(o.day, 14)
ensure_equals(o.hour, 12)
ensure_equals(o.minute, 0)
ensure_equals(o.second, 0)

-- Accessors by index
local vals = { 1789, 7, 14, 12, 0, 0 }
for idx = 1, 6 do
	ensure_equals(vals[idx], o[idx])
end

-- tostring uses iso8601 syntax
ensure_equals(tostring(o), "1789-07-14T12:00:00Z")

-- Create with current time
ensure(o < arki_time.now())

-- Create with iso8601 syntax
ensure_equals(arki_time.iso8601(tostring(o)), o)

-- Create with SQL syntax
ensure_equals(arki_time.sql("1789-07-14 12:00:00"), o)

