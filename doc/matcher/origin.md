--- Matching origins

local sample = arki.metadata.new()

-- GRIB1
-- Syntax: origin:GRIB1,centre,subcentre,process
-- Any of centre, subcentre, process can be omitted; if omitted, any value
-- will match

sample:set(arki_origin.grib1(98, 0, 12))

ensure_matches(sample, "origin:GRIB1") -- You can omit trailing commas
ensure_matches(sample, "origin:GRIB1,,,")
ensure_matches(sample, "origin:GRIB1,98")
ensure_matches(sample, "origin:GRIB1,98,,12")

ensure_not_matches(sample, "origin:GRIB2")
ensure_not_matches(sample, "origin:GRIB1,200")
ensure_not_matches(sample, "origin:GRIB1,98,0,11")

-- GRIB2
-- Syntax: origin:GRIB2,centre,subcentre,processtype,bgprocessid,processid
-- Any of centre, subcentre, processtype, bgprocessid, processid can be
-- omitted; if omitted, any value will match

sample:set(arki_origin.grib2(98, 0, 1, 2, 3))

ensure_matches(sample, "origin:GRIB2")
ensure_matches(sample, "origin:GRIB2,98")
ensure_matches(sample, "origin:GRIB2,98,,,2")
ensure_matches(sample, "origin:GRIB2,98,0,1,2,3")

ensure_not_matches(sample, "origin:GRIB1")
ensure_not_matches(sample, "origin:GRIB2,200")
ensure_not_matches(sample, "origin:GRIB2,98,1,,2")

-- BUFR
-- Syntax: origin:BUFR,centre,subcentre
-- Any of centre or subcentre can be omitted; if omitted, any value will match
-- centre: originating centre (see Common Code table C-11)
-- subcentre: originating subcentre (see Common Code table C-12)

sample:set(arki_origin.bufr(98, 0))

ensure_matches(sample, "origin:BUFR")
ensure_matches(sample, "origin:BUFR,,")
ensure_matches(sample, "origin:BUFR,98")
ensure_matches(sample, "origin:BUFR,98,0")
ensure_matches(sample, "origin:BUFR,,0")

ensure_not_matches(sample, "origin:GRIB1")
ensure_not_matches(sample, "origin:BUFR,200")
ensure_not_matches(sample, "origin:BUFR,98,1")

-- ODIMH5
-- Syntax: origin:ODIMH5,wmo,rad,nod_or_plc
-- Origin of data identified using "WMO" (wmo), "RAD" (rad) and "NOD" or "PLC"
-- (nod_or_plc) attribute values ("NOD" takes precedence over "PLC")
-- wmo, rad and nod_or_plc values can be omitted

sample:set(arki_origin.odimh5("02954", "FI44", "Anjalankoski"))

ensure_matches(sample, "origin:ODIMH5")
ensure_matches(sample, "origin:ODIMH5,02954")
ensure_matches(sample, "origin:ODIMH5,,FI44")
ensure_matches(sample, "origin:ODIMH5,,,Anjalankoski")

ensure_not_matches(sample, "origin:GRIB1")
ensure_not_matches(sample, "origin:BUFR,200")
ensure_not_matches(sample, "origin:BUFR,98,1")