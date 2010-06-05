--- Examples on how to use ensemble objects from Lua

-- GRIB ensemble

-- Create from a table
local t = { mt=1, nn=2 }
local o = arki_ensemble.grib(t)

-- The shortcut call-function-with-table Lua syntax obviously also works
ensure_equals(o, arki_ensemble.grib{mt=1, nn=2})

ensure_equals(o.style, "GRIB")

-- Values are in o.val, which creates a table with all the values
local vals = o.val
ensure_equals(vals.mt, 1)
ensure_equals(vals.nn, 2)
ensure_equals(tostring(o), "GRIB(mt=1, nn=2)")
