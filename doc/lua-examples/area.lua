--- Examples on how to use area objects from Lua

-- GRIB area

-- Create from a table
local t = { lat=45000, lon=12000 }
local o = arki_area.grib(t)

-- The shortcut call-function-with-table Lua syntax obviously also works
ensure_equals(o, arki_area.grib{lat=45000, lon=12000})

ensure_equals(o.style, "GRIB")

-- Values are in o.val, which creates a table with all the values
local vals = o.val
ensure_equals(vals.lat, 45000)
ensure_equals(vals.lon, 12000)
ensure_equals(tostring(o), "GRIB(lat=45000, lon=12000)")

-- ODIMH5 area

-- Create from a table
local t = { lat=45000, lon=12000, raiuds=1000 }
local o = arki_area.odimh5(t)

-- The shortcut call-function-with-table Lua syntax obviously also works
ensure_equals(o, arki_area.odimh5{lat=45000, lon=12000, radius=1000})

ensure_equals(o.style, "ODIMH5")

-- Values are in o.val, which creates a table with all the values
local vals = o.val
ensure_equals(vals.lat, 45000)
ensure_equals(vals.lon, 12000)
ensure_equals(tostring(o), "ODIMh5(lat=45000, lon=12000, radius=1000)")
