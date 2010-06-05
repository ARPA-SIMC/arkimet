--- Examples on how to use level objects from Lua

-- GRIB1 level

-- Create with leveltype followed by l1 and l2
-- l1 and l2 are optional, according to the level type

-- For example, surface level has no parameters
local o = arki_level.grib1(1)
ensure_equals(o.style, "GRIB1")
ensure_equals(o.type, 1)
ensure_equals(tostring(o), "GRIB1(001)")

-- For example, altitude above MSL has 1
local o = arki_level.grib1(103, 1000)
ensure_equals(o.style, "GRIB1")
ensure_equals(o.type, 103)
ensure_equals(o.l1, 1000)
ensure_equals(tostring(o), "GRIB1(103, 01000)")

-- For example, layer of altitude above MSL has 2
local o = arki_level.grib1(104, 123, 223)
ensure_equals(o.style, "GRIB1")
ensure_equals(o.type, 104)
ensure_equals(o.l1, 123)
ensure_equals(o.l2, 223)
ensure_equals(tostring(o), "GRIB1(104, 123, 223)")


-- GRIB2 single level

-- Create with type, scale and value
local o = arki_level.grib2s(103, 0, 1000)

-- Accessors
ensure_equals(o.style, "GRIB2S")
ensure_equals(o.type, 103)
ensure_equals(o.scale, 0)
ensure_equals(o.value, 1000)
ensure_equals(tostring(o), "GRIB2S(103, 000, 0000001000)")


-- GRIB2 layer

-- Create with type1, scale1, value1, type2, scale2, value2
local o = arki_level.grib2d(103, 0, 1000, 103, 0, 1100)

-- Accessors
ensure_equals(o.style, "GRIB2D")
ensure_equals(o.type1, 103)
ensure_equals(o.scale1, 0)
ensure_equals(o.value1, 1000)
ensure_equals(o.type2, 103)
ensure_equals(o.scale2, 0)
ensure_equals(o.value2, 1100)
ensure_equals(tostring(o), "GRIB2D(103, 000, 0000001000, 103, 000, 0000001100)")

