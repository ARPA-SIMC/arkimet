#ifndef ARKI_MATCHER_AREA
#define ARKI_MATCHER_AREA

#include <arki/matcher.h>
#include <arki/matcher/utils.h>
#include <arki/types/area.h>
#include <arki/utils/geosfwd.h>

namespace arki {
namespace matcher {

/**
 * Match Areas
 */
struct MatchArea : public Implementation
{
    std::string name() const override;

    static std::unique_ptr<MatchArea> parse(const std::string& pattern);
    static void init();
};

struct MatchAreaGRIB : public MatchArea
{
    types::ValueBag expr;

    MatchAreaGRIB(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
};

struct MatchAreaODIMH5 : public MatchArea
{
    types::ValueBag expr;

    MatchAreaODIMH5(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
};

struct MatchAreaVM2 : public MatchArea
{
    // This is -1 when should be ignored
    int station_id;
    types::ValueBag expr;
    std::vector<int> idlist;

    MatchAreaVM2(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
};

#ifdef HAVE_GEOS
/**
 * Match BBoxs
 */
struct MatchAreaBBox : public MatchArea
{
    arki::utils::geos::Geometry* geom;
    std::string verb;
    std::string geom_str;

    MatchAreaBBox(const std::string& verb, const std::string& geom);
    ~MatchAreaBBox();

    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
    virtual bool matchGeom(const arki::utils::geos::Geometry* val) const = 0;

    static std::unique_ptr<MatchAreaBBox> parse(const std::string& pattern);
};

struct MatchAreaBBoxEquals : public MatchAreaBBox
{
    MatchAreaBBoxEquals(const std::string& geom);
    virtual bool matchGeom(const arki::utils::geos::Geometry* val) const override;
};

struct MatchAreaBBoxIntersects : public MatchAreaBBox
{
    MatchAreaBBoxIntersects(const std::string& geom);
    virtual bool matchGeom(const arki::utils::geos::Geometry* val) const override;
};

#if GEOS_VERSION_MAJOR >= 3
struct MatchAreaBBoxCovers : public MatchAreaBBox
{
    MatchAreaBBoxCovers(const std::string& geom);
    virtual bool matchGeom(const arki::utils::geos::Geometry* val) const override;
};

struct MatchAreaBBoxCoveredBy : public MatchAreaBBox
{
    MatchAreaBBoxCoveredBy(const std::string& geom);
    virtual bool matchGeom(const arki::utils::geos::Geometry* val) const override;
};
#endif

#endif

}
}

#endif
