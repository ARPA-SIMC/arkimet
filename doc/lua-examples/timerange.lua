--- Examples on how to use timerange objects from Lua

-- GRIB1 timerange

-- Create with type, unit, p1, p2
local o = arki_timerange.grib1(0, 1, 6, 0) -- 6-hours forecast

-- Accessors (values are normalised to seconds or months)
ensure_equals(o.style, "GRIB1")
ensure_equals(o.type, 0)
ensure_equals(o.unit, "second")
ensure_equals(o.p1, 21600)
ensure_equals(o.p2, 0)
ensure_equals(tostring(o), "GRIB1(000, 006h)")


-- GRIB2 timerange (same behaviour as GRIB1)

-- Create with type, unit, p1, p2
local o = arki_timerange.grib2(0, 1, 6, 0) -- 6-hours forecast

-- Accessors (values are not normalised)
ensure_equals(o.style, "GRIB2")
ensure_equals(o.type, 0)
ensure_equals(o.unit, "h")
ensure_equals(o.p1, 6)
ensure_equals(o.p2, 0)
ensure_equals(tostring(o), "GRIB2(000, 001, 0000000006h, 0000000000h)")

