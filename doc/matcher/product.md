# Matching products

## GRIB1

Syntax: `product:GRIB1,origin,table,product`

Any of `origin`, `table`, `product` can be omitted; if omitted, any value will
match.

### Examples

Given product `GRIB1(98, 0, 12)`

* `product:GRIB1` matches
* `product:GRIB1,98` matches
* `product:GRIB1,98,,12` matches
* `product:GRIB2` does not match
* `product:GRIB1,200` does not match
* `product:GRIB1,98,0,11` does not match


## GRIB2

Syntax: `product:GRIB2,centre,discipline,category,number,tablesVersion,localTablesVersion`

Any of `centre`, `discipline`, `category`, `number`, `tablesVersion`, or
`localTablesVersion` can be omitted. If omitted, any value will match.

Given product `GRIB2(98, 0, 1, 2)`

* `product:GRIB2` matches
* `product:GRIB2,98` matches
* `product:GRIB2,98,,,2` matches
* `product:GRIB1` does not match
* `product:GRIB2,200` does not match
* `product:GRIB2,98,1,,2` does not match


## BUFR

Syntax: `product:BUFR,category,subcategory,subcategory_local:rep_memo`

Any of `category`, `subcategory`, `subcategory_local` or `rep_memo` can be
omitted; if omitted, any value will match.

* `category`: data category (BUFR or CREX Table A)
* `subcategory`: data subcategory (see Common Code table C-13)
* `subcategory_local`: data subcategory local (defined by ADP centres)

### Examples

Given product `BUFR(0, 255, 1, t=synop)`

* `product:BUFR` matches
* `product:BUFR,0,,1` matches
* `product:BUFR,0,,1:t=synop` matches
* `product:BUFR:t=synop` matches
* `product:GRIB1` does not match
* `product:BUFR:t=temp` does not match


## ODIMH5

Syntax: `product:ODIMH5,<OBJ value>,<PROD value>`

OdimH5 product, identified by object and product values.

object and product values can be omitted.

### Examples

Given product `ODIMH5(PVOL,SCAN)`

* `product:ODIMH5` matches
* `product:ODIMH5,PVOL` matches
* `product:ODIMH5,,SCAN` matches
* `product:GRIB1` does not match
* `product:BUFR:t=temp` does not match


## VM2

Syntax: `product:VM2,id:key1=value1,key2=value2,...`

The keys (`key1=value1`, …) are loaded at runtime from a meteo-vm2 attribute
table.

### Examples

Assume that product with id 23 has the following attributes:
`{bcode="B22043",tr=254,p1=0,p2=0,lt1=1}`

Given product `VM2(23)`

* `product:VM2` matches
* `product:VM2,23` matches
* `product:VM2,23:bcode=B22043` matches
* `product:GRIB1` does not match
* `product:BUFR:t=temp` does not match
* `product:VM2,12` does not match
* `product:VM2,23:foo=bar` does not match
* `product:VM2:foo=bar` does not match

[//]: # (matched: 16, not matched: 15)
