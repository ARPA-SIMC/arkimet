--- Examples on how to use origin objects from Lua

-- GRIB1 origin

-- Create with centre, subcentre, process:
local o = arki_origin.grib1(98, 0, 1)

-- Accessors
ensure_equals(o.style, "GRIB1")
ensure_equals(o.centre, 98)
ensure_equals(o.subcentre, 0)
ensure_equals(o.process, 1)
ensure_equals(tostring(o), "GRIB1(098, 000, 001)")


-- GRIB2 origin

-- Create with centre, subcentre, process type, background process id, process id:
local o = arki_origin.grib2(98, 0, 1, 2, 3)

-- Accessors
ensure_equals(o.style, "GRIB2")
ensure_equals(o.centre, 98)
ensure_equals(o.subcentre, 0)
ensure_equals(o.processtype, 1)
ensure_equals(o.bgprocessid, 2)
ensure_equals(o.processid, 3)
ensure_equals(tostring(o), "GRIB2(00098, 00000, 001, 002, 003)")


-- BUFR origin

-- Create with centre, subcentre:
local o = arki_origin.bufr(98, 0)

-- Accessors
ensure_equals(o.style, "BUFR")
ensure_equals(o.centre, 98)
ensure_equals(o.subcentre, 0)
ensure_equals(tostring(o), "BUFR(098, 000)")


-- ODIMH5 origin

-- Create with wmo, rad and plc values
local o = arki_origin.odimh5(1, 2, 3)

-- Accessors
ensure_equals(o.wmo, "1")
ensure_equals(o.rad, "2")
ensure_equals(o.plc, "3")
ensure_equals(tostring(o), "ODIMH5(1, 2, 3)")

