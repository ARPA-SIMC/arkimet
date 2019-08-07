#include "config.h"
#include "arki/bbox.h"
#include "arki/exceptions.h"
#include "arki/utils/geos.h"
#include "arki/utils/string.h"
#include "arki/runtime.h"
#include <memory>

using namespace std;

namespace arki {

BBox::BBox(const std::string& code) : L(new Lua), funcCount(0)
{
    /// Load the bounding box functions

    /// Load the bbox scanning functions
    if (code.empty())
    {
        vector<string> sources = arki::Config::get().dir_bbox.list_files(".lua");
        for (const auto& source: sources)
        {
            char buf[32];
            snprintf(buf, 32, "BBOX_%u", funcCount++);
            L->functionFromFile(buf, source);
        }
    } else {
        char buf[32];
        snprintf(buf, 32, "BBOX_%u", funcCount++);
        L->functionFromString(buf, code, "bbox scan instructions");
    }
}

BBox::~BBox()
{
    if (L) delete L;
}

#ifdef HAVE_GEOS
static vector< pair<double, double> > bbox(lua_State* L)
{
    lua_getglobal(L, "bbox");
    int type = lua_type(L, -1);
    switch (type)
    {
        case LUA_TNIL: {
            lua_pop(L, 1);
            throw std::runtime_error("cannot read bounding box: global variable 'bbox' has not been set");
        }
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
                    stringstream ss;
                    ss << "cannot read bounding box: value bbox[" << i << "] contains a " << lua_typename(L, inner_type) << " instead of a table";
                    throw std::runtime_error(ss.str());
                }
                double vals[2];
                for (int j = 0; j < 2; ++j)
                {
                    lua_rawgeti(L, -1, j+1);
                    if (lua_type(L, -1) != LUA_TNUMBER)
                    {
                        lua_pop(L, 3);
                        stringstream ss;
                        ss << "cannot read bounding box: value bbox[" << i << "][" << j << "] is not a number";
                        throw std::runtime_error(ss.str());
                    }
                    vals[j] = lua_tonumber(L, -1);
                    lua_pop(L, 1);
                }
                res.push_back(make_pair(vals[0], vals[1]));
                lua_pop(L, 1);
            }
            return res;
        }
        default: {
            lua_pop(L, 1);
            stringstream ss;
            ss << "cannot read bounding box: global variable 'bbox' has type " << lua_typename(L, type) << " instead of table";
            throw std::runtime_error(ss.str());
        }
    }
}
#endif

std::unique_ptr<arki::utils::geos::Geometry> BBox::operator()(const types::Area& v) const
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
        throw_consistency_error("computing bounding box via Lua", error);
    } else {
        vector< pair<double, double> > points = bbox(*L);
        auto gf = arki::utils::geos::GeometryFactory::getDefaultInstance();
		switch (points.size())
		{
			case 0:
				return unique_ptr<arki::utils::geos::Geometry>();
			case 1:
				return unique_ptr<arki::utils::geos::Geometry>(
						gf->createPoint(arki::utils::geos::Coordinate(points[0].second, points[0].first)));
			default:
                arki::utils::geos::CoordinateArraySequence cas;
                for (size_t i = 0; i < points.size(); ++i)
                    cas.add(arki::utils::geos::Coordinate(points[i].second, points[i].first));
                unique_ptr<arki::utils::geos::LinearRing> lr(gf->createLinearRing(cas));
                return unique_ptr<arki::utils::geos::Geometry>(
                        gf->createPolygon(*lr, vector<arki::utils::geos::Geometry*>()));
        }
    }
#else
    return unique_ptr<arki::utils::geos::Geometry>();
#endif
}

const BBox& BBox::get_singleton()
{
    static __thread BBox* bbox = 0;
    if (!bbox)
        bbox = new BBox();
    return *bbox;
}

}
