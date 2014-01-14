/*
 * arki/bbox - Compute bounding boxes
 *
 * Copyright (C) 2009--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include "config.h"

#include <arki/bbox.h>
#include <arki/utils/geosdef.h>
#include <arki/runtime/config.h>
#include <wibble/exception.h>
#include <wibble/string.h>
#include <memory>

using namespace ARKI_GEOS_NS;

using namespace std;
using namespace wibble;

namespace arki {

#if 0
static string arkilua_dumptablekeys(lua_State* L, int index)
{
	string res;
	// Start iteration
	lua_pushnil(L);
	while (lua_next(L, index) != 0)
	{
		if (!res.empty()) res += ", ";
		// key is at index -2 and we want it to be a string
		if (lua_type(L, -2) != LUA_TSTRING)
			res += "<not a string>";
		else
			res += lua_tostring(L, -2);
		lua_pop(L, 1);
	}
	return res;
}

static void arkilua_dumpstack(lua_State* L, const std::string& title, FILE* out)
{
	fprintf(out, "%s\n", title.c_str());
	for (int i = lua_gettop(L); i; --i)
	{
		int t = lua_type(L, i);
		switch (t)
		{
			case LUA_TNIL:
				fprintf(out, " %d: nil\n", i);
				break;
			case LUA_TBOOLEAN:
				fprintf(out, " %d: %s\n", i, lua_toboolean(L, i) ? "true" : "false");
				break;
			case LUA_TNUMBER:
				fprintf(out, " %d: %g\n", i, lua_tonumber(L, i));
				break;
			case LUA_TSTRING:
				fprintf(out, " %d: '%s'\n", i, lua_tostring(L, i));
				break;
			case LUA_TTABLE:
				fprintf(out, " %d: table: '%s'\n", i, arkilua_dumptablekeys(L, i).c_str());
				break;
			default:
				fprintf(out, " %d: <%s>\n", i, lua_typename(L, t));
				break;
		}
	}
}
#endif


BBox::BBox(const std::string& code) : L(new Lua), gf(new ARKI_GEOS_GEOMETRYFACTORY), funcCount(0)
{
	/// Load the bounding box functions

	/// Load the bbox scanning functions
	if (code.empty())
	{
		vector<string> files = runtime::rcFiles("bbox", "ARKI_BBOX");
		for (vector<string>::const_iterator i = files.begin(); i != files.end(); ++i)
			L->functionFromFile("BBOX_" + str::fmt(funcCount++), *i);
	} else
		L->functionFromString("BBOX_" + str::fmt(funcCount++), code, "bbox scan instructions");

	//arkilua_dumpstack(L, "Afterinit", stderr);
}

BBox::~BBox()
{
	if (L) delete L;
	if (gf) delete gf;
}

static vector< pair<double, double> > bbox(lua_State* L)
{
	lua_getglobal(L, "bbox");
	int type = lua_type(L, -1);
	switch (type)
	{
		case LUA_TNIL:
			lua_pop(L, 1);
			throw wibble::exception::Consistency("reading bounding box", "global variable 'bbox' has not been set");
		case LUA_TTABLE: {
			vector< pair<double, double> > res;
#if LUA_VERSION_NUM >= 502
			size_t asize = lua_rawlen(L, -1);
#else
			size_t asize = lua_objlen(L, -1);
#endif
			for (size_t i = 1; i <= asize; ++i)
			{
				lua_rawgeti(L, -1, i);
				int inner_type = lua_type(L, -1); 
				if (inner_type != LUA_TTABLE)
				{
					lua_pop(L, 2);
					throw wibble::exception::Consistency("reading bounding box",
						"value bbox[" + str::fmt(i) + "] contains a " + lua_typename(L, inner_type) + " instead of a table");
				}
				double vals[2];
				for (int j = 0; j < 2; ++j)
				{
					lua_rawgeti(L, -1, j+1);
					if (lua_type(L, -1) != LUA_TNUMBER)
					{
						lua_pop(L, 3);
						throw wibble::exception::Consistency("reading bounding box",
							"value bbox[" + str::fmt(i) + "][" + str::fmt(j) + "] is not a number");
					}
					vals[j] = lua_tonumber(L, -1);
					lua_pop(L, 1);
				}
				res.push_back(make_pair(vals[0], vals[1]));
				lua_pop(L, 1);
			}
			return res;
		}
		default:
			lua_pop(L, 1);
			throw wibble::exception::Consistency("reading bounding box", string("global variable 'bbox' has type ") + lua_typename(L, type) + " instead of table");
	}
}

auto_ptr<ARKI_GEOS_GEOMETRY> BBox::operator()(const Item<types::Area>& v) const
{
	return (*this)(*v);
}

std::auto_ptr<ARKI_GEOS_GEOMETRY> BBox::operator()(const types::Area& v) const
{
#ifdef HAVE_GEOS
	// Set the area information as the 'area' global
	v.lua_push(*L);
	lua_setglobal(*L, "area");
	lua_newtable(*L);
	lua_setglobal(*L, "bbox");

	std::string error = L->runFunctionSequence("BBOX_", funcCount);
	if (!error.empty())
	{
		throw wibble::exception::Consistency("computing bounding box via Lua", error);
	} else {
		//arkilua_dumpstack(L, "Afterscan", stderr);
		vector< pair<double, double> > points = bbox(*L);
		switch (points.size())
		{
			case 0:
				return auto_ptr<ARKI_GEOS_GEOMETRY>(0);
			case 1:
				return auto_ptr<ARKI_GEOS_GEOMETRY>(
						gf->createPoint(Coordinate(points[0].second, points[0].first)));
			default:
				CoordinateArraySequence cas;
				for (size_t i = 0; i < points.size(); ++i)
					cas.add(Coordinate(points[i].second, points[i].first));
				auto_ptr<LinearRing> lr(gf->createLinearRing(cas));
				return auto_ptr<ARKI_GEOS_GEOMETRY>(
						gf->createPolygon(*lr, vector<Geometry*>()));
		}
	}
#else
	return auto_ptr<ARKI_GEOS_GEOMETRY>(0);
#endif
}

}
// vim:set ts=4 sw=4:
