--- Examples on how to use run objects from Lua

-- Minute run

-- Create with just hour
local o = arki_run.minute(12)

-- Create with hour and minute
ensure_equals(o, arki_run.minute(12, 0))

-- Accessors
ensure_equals(o.style, "MINUTE")
ensure_equals(o.hour, 12)
ensure_equals(o.min, 0)
ensure_equals(tostring(o), "MINUTE(12:00)")
