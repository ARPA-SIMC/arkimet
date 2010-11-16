--- Examples on how to use metadata objects from Lua

-- arki.metadata.new() creates a new empty metadata

local md = arki.metadata.new()

-- Metadata items can be set with md:set():

local area = arki_area.grib{lat=45000, lon=12000}
md:set(area)

-- They can be accessed by name:

ensure_equals(md.area, area)

-- And they can be unset with md::unset()

md:unset("area")
ensure_equals(md.area, nil)

-- Notes are accessed with md:notes(), as a table

ensure_equals(table.getn(md:notes()), 0)

-- TODO: there is no currently implemented way to set notes

-- It is also possible to set items by name:

md.area = area
ensure_equals(md.area, area)

-- Metadata can be copied with md:copy()

local md1 = md:copy()
ensure_equals(md, md1)

-- md:data() gives a table with the metadata contents, which is useful for
-- iteration

local found = 0
for name, item in pairs(md:data()) do
	if name == "area" then
		ensure_equals(item, area)
		found = found + 1
	elseif name == "notes" then
		ensure_equals(table.getn(item), 0)
		found = found + 1
	end
end
ensure_equals(found, 2)

--[[
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
local t = { lat=45000, lon=12000, radius=1000 }
local o = arki_area.odimh5(t)

-- The shortcut call-function-with-table Lua syntax obviously also works
ensure_equals(o, arki_area.odimh5{lat=45000, lon=12000, radius=1000})

ensure_equals(o.style, "ODIMH5")

-- Values are in o.val, which creates a table with all the values
local vals = o.val
ensure_equals(vals.lat, 45000)
ensure_equals(vals.lon, 12000)
ensure_equals(tostring(o), "ODIMH5(lat=45000, lon=12000, radius=1000)")
]]
