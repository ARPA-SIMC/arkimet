# Matching areas

## Matching GRIB areas

Syntax: `area:GRIB:key1=val1,key2=val2,...`

Matches any GRIB area that has at least all the given keys, with the
given values.

There is no exaustive list of possible keys, as they can be anything that
gets set by the GRIB scanning configuration.

### Examples

Given area `GRIB(lat=45000, lon=12000)`

* `area:GRIB:lat=45000` matches
* `area:GRIB:lat=45000,lon=12000` matches
* `area:GRIB:lat=45,lon=12` does not match
* `area:GRIB:answer=42` does not match

Given area `GRIB(blo=10, lat=5480000, lon=895000, sta=22)`

* `area:GRIB:sta=22` matches
* `area:GRIB:sta=88` does not match


## Matching ODIMH5 areas

Syntax: `area:ODIMH5:lat=value,lon=value,radius=value`

Latitude and longitude are degrees in decimal notation mutiplied by 1000000
For example 12.345 degrees becomes "12345000" 

Radius is a value expressed in meters

bbox for ODIMH5 PVOL files are calculated building an octagon that contains
a circle centered at latitude and longitude with the given radius.

The octagon has some extra areas that exceed the original circle so
so particular attention must be paid by user.

### Examples

Given area `ODIMH5(lat=45000000, lon=30000000, radius=1000)`

* `area:ODIMH5:lat=45000000,lon=30000000,radius=1000` matches
* `area:GRIB:sta=88` does not match


## Matching VM2 areas

Syntax: `area:VM2,id:key1=val1,key2=val2,...`

Matches any VM2 area that has the at least the given id and all the given keys,
with the given values.

The attributes (`key1=value1`, …) are loaded at runtime from a meteo-vm2
attribute table.

### Examples

Assume that area with id 1 has the attributes `{lon=1207738,lat=4460016,rep="locali"}`

Given area `VM2(1)`

* `area:VM2,1` matches
* `area:VM2:lon=1207738,lat=4460016,rep=locali` matches
* `area:VM2:rep=locali` matches
* `area:VM2,2` does not match
* `area:VM2:answer=42` does not match


## Matching area bounding boxes

Syntax: `area:bbox {verb} {value}`

Matches the area bounding box against a sample value. The sample value is
represented by a WKT geometry.

Available verbs:

* `equals`: the bounding box to match must be the same as the sample value
* `covers`: the bounding box to match must contain the sample value
* `coveredby`: the bounding box to match must be contained inside the sample value
* `intersects`: the bounding box to match must intersect the sample value

See: <http://www.cs.montana.edu/courses/535/resources/Understanding%20spatial%20relations.htm>

### Examples

Given area `GRIB(Ni=441, Nj=181, latfirst=45000000, latlast=43000000, lonfirst=10000000, lonlast=12000000, type=0)`

`equals`:

* `area: bbox equals POLYGON((10 43, 12 43, 12 45, 10 45, 10 43))` matches
* `area: bbox equals POINT EMPTY` does not match
* `area: bbox equals POINT(11 44)` does not match
* `area: bbox equals POLYGON((12 43, 10 44, 12 45, 12 43))` does not match

`covers`:

* `area: bbox covers POINT(11 44)` matches
* `area: bbox covers POINT(12 43)` matches
* `area: bbox covers LINESTRING(10 43, 10 45, 12 45, 12 43, 10 43)` matches
* `area: bbox covers POLYGON((10 43, 10 45, 12 45, 12 43, 10 43))` matches
* `area: bbox covers POLYGON((10.5 43.5, 10.5 44.5, 11.5 44.5, 11.5 43.5, 10.5 43.5))` matches
* `area: bbox covers POLYGON((12 43, 10 44, 12 45, 12 43))` matches
* `area: bbox covers POINT(11 40)` does not match
* `area: bbox covers POINT(13 44)` does not match
* `area: bbox covers POINT( 5 40)` does not match
* `area: bbox covers LINESTRING(10 43, 10 45, 13 45, 13 43, 10 43)` does not match
* `area: bbox covers POLYGON((10 43, 10 45, 13 45, 13 43, 10 43))` does not match
* `area: bbox covers POLYGON((12 42, 10 44, 12 45, 12 42))` does not match

`intersects`:
* `area: bbox intersects POINT(11 44)` matches
* `area: bbox intersects POINT(12 43)` matches
* `area: bbox intersects LINESTRING(10 43, 10 45, 12 45, 12 43, 10 43)` matches
* `area: bbox intersects POLYGON((10 43, 10 45, 12 45, 12 43, 10 43))` matches
* `area: bbox intersects POLYGON((10.5 43.5, 10.5 44.5, 11.5 44.5, 11.5 43.5, 10.5 43.5))` matches
* `area: bbox intersects POLYGON((12 43, 10 44, 12 45, 12 43))` matches
* `area: bbox intersects POLYGON((10 43, 10 45, 13 45, 13 43, 10 43))` matches
* `area: bbox intersects POLYGON((12 42, 10 44, 12 45, 12 42))` matches
* `area: bbox intersects POLYGON((9 40, 10 43, 10 40, 9 40))` matches: Touches one vertex
* `area: bbox intersects POINT(11 40)` does not match -- Outside
* `area: bbox intersects POINT(13 44)` does not match -- Outside
* `area: bbox intersects POINT( 5 40)` does not match -- Outside
* `area: bbox intersects LINESTRING(9 40, 10 42, 10 40, 9 40)` does not match -- Disjoint
* `area: bbox intersects POLYGON((9 40, 10 42, 10 40, 9 40))` does not match -- Disjoint

`coveredby`:

* `area: bbox coveredby POLYGON((10 43, 10 45, 12 45, 12 43, 10 43))` matches: Same shape
* `area: bbox coveredby POLYGON((10 43, 10 45, 13 45, 13 43, 10 43))` matches: Trapezoid containing area
* `area: bbox coveredby POINT(11 44)` does not match: Intersection exists but area is 0
* `area: bbox coveredby POINT(12 43)` does not match: Intersection exists but area is 0
* `area: bbox coveredby POINT(11 40)` does not match: Outside
* `area: bbox coveredby POINT(13 44)` does not match: Outside
* `area: bbox coveredby POINT( 5 40)` does not match: Outside
* `area: bbox coveredby POLYGON((12 42, 10 44, 12 45, 12 42))` does not match
* `area: bbox coveredby POLYGON((12 43, 10 44, 12 45, 12 43))` does not match: Triangle contained inside, touching borders (?)
* `area: bbox coveredby POLYGON((10.5 43.5, 10.5 44.5, 11.5 44.5, 11.5 43.5, 10.5 43.5))` does not match: Fully inside
* `area: bbox coveredby LINESTRING(10 43, 10 45, 12 45, 12 43, 10 43)` does not match:Same shape, but intersection area is 0
* `area: bbox coveredby POLYGON((9 40, 10 43, 10 40, 9 40))` does not match: Touches one vertex
* `area: bbox coveredby LINESTRING(9 40, 10 42, 10 40, 9 40)` does not match:Disjoint
* `area: bbox coveredby POLYGON((9 40, 10 42, 10 40, 9 40))` does not match: Disjoint

[//]: # (matched: 25, not matched: 32)
