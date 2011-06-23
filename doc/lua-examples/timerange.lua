--- Examples on how to use timerange objects from Lua

-- GRIB1 timerange

-- Create with type, unit, p1, p2
local o = arki_timerange.grib1(0, 1, 6, 0) -- 6-hours forecast

-- Accessors (values are normalised to seconds or months)
ensure_equals(o.style, "GRIB1")
ensure_equals(o.type, 0)
ensure_equals(o.unit, "second")
ensure_equals(o.p1, 21600)
ensure_equals(o.p2, nil) -- p2 is meaningless with type=0
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


--[[ Timedef timerange

Timedef time ranges have 2 parts: the forecast step and the information on
statistical processing.

The forecast step is a duration and a time unit.

The statistical processing has a type of statistical processing and an interval
length, again consisting of a duration and a time unit.

Time units can be specified numerically using values from GRIB code table 4.4,
or with string abbreviations:

    s: Second
    m: Minute
    h: Hour
    d: Day
   mo: Month
    y: Year
   de: Decade
   no: Normal
   ce: Century
   h3: 3 hours
   h6: 6 hours
  h12: 12 hours

The statistical processing information can be missing.
]]

-- Create with only forecast step
local o = arki_timerange.timedef(6, "h") -- 6-hours forecast
-- Also works as a string
local o = arki_timerange.timedef("6h") -- 6-hours forecast

-- Create with forecast step and statistical processing
local o = arki_timerange.timedef(6, "h", 2, 3, "h") -- +6h forecast, maximum over 3h
-- Also works with strings
local o = arki_timerange.timedef("6h", 2, "3h") -- +6h forecast, maximum over 3h

-- Create as an analysis with statistical processing
local o = arki_timerange.timedef(0, "s", 2, 3, "h") -- maximum over 3h

-- Accessors (values are not normalised)
ensure_equals(o.style, "Timedef")
ensure_equals(o.step_unit, 13)
ensure_equals(o.step_len, 0)
ensure_equals(o.stat_type, 2)
ensure_equals(o.stat_unit, 1)
ensure_equals(o.stat_len, 3)
ensure_equals(tostring(o), "Timedef(0s, 2, 3h)")


-- BUFR timerange (only with forecast time)

-- Create with type, unit, p1, p2
local o = arki_timerange.bufr(6, 1) -- 6-hours forecast

-- Accessors
ensure_equals(o.style, "BUFR")
ensure_equals(o.unit, 1)
ensure_equals(o.value, 6)
-- A value is either convertible to seconds or convertible to months
ensure_equals(o.is_seconds, true)
ensure_equals(o.seconds, 6*3600)
ensure_equals(o.months, nil)
ensure_equals(tostring(o), "BUFR(6h)")

