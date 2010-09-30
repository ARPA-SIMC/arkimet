--- Examples on how to use quantity objects from Lua

-- QUANTITY 

-- Create with origin, table, product
local o = arki_quantity("VRAD", "WRAD")

-- Accessors
ensure_equals(tostring(o), "VRAD, WRAD")


