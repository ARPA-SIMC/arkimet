#include "config.h"
#include "arki/bbox.h"
#include "arki/exceptions.h"
#include "arki/utils/geos.h"
#include "arki/utils/string.h"
#include "arki/runtime.h"
#include <memory>
#include <map>

using namespace std;

namespace arki {

namespace {

struct NullBBox : public BBox
{
    std::unique_ptr<arki::utils::geos::Geometry> compute(const types::Area& v) const override
    {
        return unique_ptr<arki::utils::geos::Geometry>();
    }
};

struct MockBBox : public BBox
{
    mutable std::map<std::string, std::string> db;
    mutable geos::io::WKTReader* reader = nullptr;

    MockBBox()
    {
    }

    ~MockBBox()
    {
        delete reader;
    }

    void init() const
    {
        reader = new geos::io::WKTReader;
        // Fill db
        db["GRIB(Ni=441, Nj=181, latfirst=45000000, latlast=43000000, lonfirst=10000000, lonlast=12000000, type=0)"] =
            "POLYGON ((10 45, 10 43, 12 43, 12 45, 10 45))",
        db["GRIB(Ni=441, Nj=181, latfirst=75000000, latlast=30000000, lonfirst=-45000000, lonlast=65000000, type=0)"] =
            "POLYGON ((-45 75, -45 30, 65 30, 65 75, -45 75))";
        db["GRIB(blo=0, lat=4502770, lon=966670, sta=101)"] =
            "POINT (9.666700000000001 45.0277)";
        db["ODIMH5(lat=44456700, lon=11623600, radius=100000)"] =
            "POLYGON ((11.9960521084854 45.35574923752996, 12.52266823017452 44.82910625443706, 12.52266823017452 44.08429374556294, 11.9960521084854 43.55765076247004, 11.2511478915146 43.55765076247004, 10.72453176982548 44.08429374556294, 10.72453176982548 44.82910625443706, 11.2511478915146 45.35574923752996, 11.9960521084854 45.35574923752996))";
    }

    std::unique_ptr<arki::utils::geos::Geometry> compute(const types::Area& v) const override
    {
        // Lazily load area values used in tests
        if (db.empty())
            init();

        std::string key = v.to_string();
        auto val = db.find(key);
        if (val == db.end())
        {
            fprintf(stderr, "MOCK MISSING %s\n", key.c_str());
            return std::unique_ptr<arki::utils::geos::Geometry>();
        }
        else
        {
            return std::unique_ptr<arki::utils::geos::Geometry>(reader->read(val->second));
        }
    }
};

}

static std::function<std::unique_ptr<BBox>()> factory;

BBox::~BBox()
{
}


unique_ptr<BBox> BBox::create()
{
    if (factory)
        return factory();
    return unique_ptr<BBox>(new MockBBox);
}

void BBox::set_factory(std::function<std::unique_ptr<BBox>()> new_factory)
{
    factory = new_factory;
}

}
