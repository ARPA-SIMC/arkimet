#ifndef ARKI_MATCHER_AREA
#define ARKI_MATCHER_AREA

#include <arki/matcher/utils.h>
#include <arki/types/values.h>
#include <arki/utils/geos.h>

namespace arki {
namespace matcher {

/**
 * Match Areas
 */
struct MatchArea : public Implementation
{
    MatchArea* clone() const override = 0;
    std::string name() const override;

    static Implementation* parse(const std::string& pattern);
    static void init();
};

struct MatchAreaGRIB : public MatchArea
{
    types::ValueBagMatcher expr;

    MatchAreaGRIB(const types::ValueBagMatcher& expr);
    MatchAreaGRIB(const std::string& pattern);
    MatchAreaGRIB* clone() const override;
    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data,
                      unsigned size) const override;
    std::string toString() const override;
};

struct MatchAreaODIMH5 : public MatchArea
{
    types::ValueBagMatcher expr;

    MatchAreaODIMH5(const types::ValueBagMatcher& expr);
    MatchAreaODIMH5(const std::string& pattern);
    MatchAreaODIMH5* clone() const override;
    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data,
                      unsigned size) const override;
    std::string toString() const override;
};

struct MatchAreaVM2 : public MatchArea
{
    // This is -1 when should be ignored
    int station_id;
    types::ValueBagMatcher expr;
    std::vector<int> idlist;

    MatchAreaVM2(const MatchAreaVM2& o);
    MatchAreaVM2(const std::string& pattern);
    MatchAreaVM2* clone() const override;
    bool matchItem(const types::Type& o) const override;
    bool match_buffer(types::Code code, const uint8_t* data,
                      unsigned size) const override;
    std::string toString() const override;
};

#ifdef HAVE_GEOS
/**
 * Match BBoxs
 */
struct MatchAreaBBox : public MatchArea
{
    arki::utils::geos::Geometry geom;
    std::string verb;
    std::string geom_str;

    MatchAreaBBox(const MatchAreaBBox& o);
    MatchAreaBBox(const std::string& verb, const std::string& geom);
    ~MatchAreaBBox();

    MatchAreaBBox* clone() const override = 0;
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
    virtual bool matchGeom(const arki::utils::geos::Geometry& val) const = 0;

    static Implementation* parse(const std::string& pattern);
};

struct MatchAreaBBoxEquals : public MatchAreaBBox
{
    using MatchAreaBBox::MatchAreaBBox;
    MatchAreaBBoxEquals(const std::string& geom);
    MatchAreaBBoxEquals* clone() const override;
    virtual bool
    matchGeom(const arki::utils::geos::Geometry& val) const override;
};

struct MatchAreaBBoxIntersects : public MatchAreaBBox
{
    using MatchAreaBBox::MatchAreaBBox;
    MatchAreaBBoxIntersects(const std::string& geom);
    MatchAreaBBoxIntersects* clone() const override;
    virtual bool
    matchGeom(const arki::utils::geos::Geometry& val) const override;
};

#if GEOS_VERSION_MAJOR >= 3
struct MatchAreaBBoxCovers : public MatchAreaBBox
{
    using MatchAreaBBox::MatchAreaBBox;
    MatchAreaBBoxCovers(const std::string& geom);
    MatchAreaBBoxCovers* clone() const override;
    virtual bool
    matchGeom(const arki::utils::geos::Geometry& val) const override;
};

struct MatchAreaBBoxCoveredBy : public MatchAreaBBox
{
    using MatchAreaBBox::MatchAreaBBox;
    MatchAreaBBoxCoveredBy(const std::string& geom);
    MatchAreaBBoxCoveredBy* clone() const override;
    virtual bool
    matchGeom(const arki::utils::geos::Geometry& val) const override;
};
#endif

#endif

} // namespace matcher
} // namespace arki

#endif
