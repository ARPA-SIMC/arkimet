--[[ Using aliases

For every metadata type it is possible to define some aliases for the most
commonly used search expressions.

Aliases are defined in /etc/arkimet/match-alias.conf, which has one section per
metadata type. For example:

  [origin]
  arpa = GRIB1,200

  [product]
  # Pressure
  pr      = GRIB1,200,2,1
  # MSL pressure
  mslp    = GRIB1,200,2,2  or GRIB1,98,128,151
  # Geopotential
  z       = GRIB1,200,2,6  or GRIB1,98,128,129
  # Temperature
  t       = GRIB1,200,2,11 or GRIB1,98,128,130 or GRIB1,98,128,167 or GRIB1,200,200,11
  
  [level]
  # Surface
  g00    = GRIB1,1                                              
  
  # Height from surface
  g02    = GRIB1,105,2                                            
  g10    = GRIB1,105,10
]]

local sample = arki.metadata.new()

sample:set(arki_origin.grib1(200, 0, 0))
sample:set(arki_product.grib1(98, 128, 167))
sample:set(arki_level.grib1(1))

ensure_matches(sample, "origin:arpa")
ensure_matches(sample, "origin:arpa or GRIB1,98,0")
ensure_matches(sample, "product:t")
ensure_matches(sample, "product:t or z")
ensure_matches(sample, "origin:arpa; product:t; level:g00 or g02")
