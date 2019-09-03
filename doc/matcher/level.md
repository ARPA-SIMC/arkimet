# Matching levels

## GRIB1

Syntax: `level:GRIB1,leveltype,l1,l2`

Any of `leveltype`, `l1`, or `l2` can be omitted; if omitted, any value will
match.

### Examples

Given level `GRIB1(1)` (Surface level, with no parameters)

* `level:GRIB1` matches
* `level:GRIB1,1` matches
* `level:GRIB1,2` does not match
* `level:GRIB1,1,0` does not match

Given level `GRIB1(103, 1000)` (Simple level: altitude above MSL)

* `level:GRIB1` matches
* `level:GRIB1,103` matches
* `level:GRIB1,103,1000` matches
* `level:GRIB1,1` does not match
* `level:GRIB1,103,1001` does not match
* `level:GRIB1,103,1000,1000` does not match

Given level `GRIB1(104, 123, 223)` (Layer between two altitudes)

* `level:GRIB1` matches
* `level:GRIB1,104` matches
* `level:GRIB1,104,,223` matches
* `level:GRIB1,104,123` matches
* `level:GRIB1,104,123,223` matches
* `level:GRIB1,103,123` does not match
* `level:GRIB1,104,123,222` does not match

## GRIB2 single level

Syntax: `level:GRIB2S,type,scale,value`

Any of `type`, `scale`, or `value` can be omitted; if omitted, any value will
match.


### Examples

Given level `GRIB2S(103, 0, 1000)`

* `level:GRIB2S` matches
* `level:GRIB2S,103` matches
* `level:GRIB2S,103,0,1000` matches
* `level:GRIB2S,103,0` matches
* `level:GRIB2S,103,,1000` matches
* `level:GRIB2S,103,1,1000` does not match
* `level:GRIB2S,103,0,1001` does not match


## GRIB2 layer

Syntax: `level:GRIB2D,type1,scale1,value1,type2,scale2,value2`

Any of the parameters can be omitted; if omitted, any value will match.

Given level `GRIB2D(103, 0, 1000, 103, 0, 1100)`

* `level:GRIB2D` matches
* `level:GRIB2D,103` matches
* `level:GRIB2D,103,0,1000` matches
* `level:GRIB2D,103,0` matches
* `level:GRIB2D,103,,1000` matches
* `level:GRIB2D,103,,,103,,` matches
* `level:GRIB2D,103,0,1000,103,0,1100` matches
* `level:GRIB2S,103,0,1000` does not match
* `level:GRIB2D,102` does not match
* `level:GRIB2D,103,1,1000,103,1,1100` does not match


## ODIMH5 layer

Syntax: `level:ODIMH5,val1 val2 val3 .... [offset value]`

Match any elevation angle equal to given values, offset value is the maximum
allowed tollerance.

Syntax: `level:ODIMH5,range minval maxval`

Match any elevation between the given min and max values.

### Examples

Given level `ODIMH5(0.5, 5.5, 10.5)`

* `level:ODIMH5, 0.5` matches
* `level:ODIMH5, 0.5 5.5 10.5 offset 0.5` matches
* `level:ODIMH5, range 5 20` matches
* `level:GRIB2S,103,0,1000` does not match
* `level:GRIB2D,102` does not match
* `level:GRIB2D,103,1,1000,103,1,1100` does not match

[//]: # (matched: 25, not matched: 15)
