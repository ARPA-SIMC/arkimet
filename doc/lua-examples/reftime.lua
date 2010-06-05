--- Examples on how to use reftime objects from Lua

-- GRIB1 reftime

-- Create an instant in time, from one arki_time object
local o = arki_reftime.position(arki_time.sql("2010-09-08 07:06:05"))

-- Accessors
ensure_equals(o.style, "POSITION")
ensure_equals(o.time, arki_time.sql("2010-09-08 07:06:05"))
ensure_equals(tostring(o), "2010-09-08T07:06:05Z")


-- Create a time period, between two arki_time objects (extremes included)
local o = arki_reftime.period(arki_time.sql("2010-09-08 07:06:05"), arki_time.sql("2011-10-09 08:07:06"))

-- Accessors
ensure_equals(o.style, "PERIOD")
ensure_equals(o.from, arki_time.sql("2010-09-08 07:06:05"))
ensure_equals(o.to, arki_time.sql("2011-10-09 08:07:06"))
ensure_equals(tostring(o), "2010-09-08T07:06:05Z to 2011-10-09T08:07:06Z")
