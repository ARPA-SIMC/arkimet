--- Examples on how to use quantity objects from Lua

-- QUANTITY 

-- Create with one value per parameter
local o = arki_quantity.new("VRAD", "WRAD")

-- Accessors
ensure_equals(tostring(o), "VRAD, WRAD")


-- Create with an array of values
local o = arki_quantity.new{"VRAD", "WRAD"}

-- Accessors
ensure_equals(tostring(o), "VRAD, WRAD")

