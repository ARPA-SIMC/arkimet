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

-- Create with product name
local o = arki_product.bufr("synop")

-- Accessors
ensure_equals(o.style, "BUFR")
ensure_equals(o.name, "synop")
ensure_equals(tostring(o), "BUFR(synop)")

