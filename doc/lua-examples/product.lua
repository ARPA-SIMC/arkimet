--- Examples on how to use product objects from Lua

-- GRIB1 product

-- Create with origin, table, product
local o = arki_product.grib1(98, 0, 1)

-- Accessors
ensure_equals(o.style, "GRIB1")
ensure_equals(o.origin, 98)
ensure_equals(o.table, 0)
ensure_equals(o.product, 1)
ensure_equals(tostring(o), "GRIB1(098, 000, 001)")


-- GRIB2 product

-- Create with centre, discipline, category, number:
local o = arki_product.grib2(98, 0, 1, 2)

-- Accessors
ensure_equals(o.style, "GRIB2")
ensure_equals(o.centre, 98)
ensure_equals(o.discipline, 0)
ensure_equals(o.category, 1)
ensure_equals(o.number, 2)
ensure_equals(tostring(o), "GRIB2(00098, 000, 001, 002)")


-- BUFR product

-- Create with type, subtype, localsubtype from section 1
local o = arki_product.bufr(0, 255, 1)
ensure_equals(o.style, "BUFR")
ensure_equals(o.type, 0)
ensure_equals(o.subtype, 255)
ensure_equals(o.localsubtype, 1)
ensure_equals(tostring(o), "BUFR(000, 255, 001)")

-- It can also have key=value pairs attached
local o = arki_product.bufr(0, 255, 1, {t="synop"})
ensure_equals(o.style, "BUFR")
ensure_equals(o.type, 0)
ensure_equals(o.subtype, 255)
ensure_equals(o.localsubtype, 1)
ensure_equals(o.val.t, "synop")
ensure_equals(tostring(o), "BUFR(000, 255, 001, t=synop)")

-- It is possible to add/replace key=value pairs
local o1 = arki_product.bufr(0, 255, 1)
o1 = o:addValues{t="synop"}
ensure_equals(o, o1)
