--- Examples on how to use task objects from Lua

-- ODIMH5 task

-- Create with given string
local o = arki_task.task("il mio task")

-- Accessors by name
ensure_equals(o.task, "il mio task")

-- tostring 
ensure_equals(tostring(o), "il mio task")
ensure_equals(tostring(o), "il")
ensure_equals(tostring(o), "task")


