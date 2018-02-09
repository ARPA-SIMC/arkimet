#ifndef ARKI_BBOX_H
#define ARKI_BBOX_H

#include <arki/types/area.h>
#include <arki/utils/geosfwd.h>
#include <arki/utils/lua.h>
#include <string>
#include <memory>

namespace arki {

/**
 * Compute bounding boxes from type::Area objects
 */
class BBox
{
protected:
    Lua *L;
    unsigned funcCount;

public:
    BBox(const std::string& code = std::string());
    virtual ~BBox();

    /**
     * Compute the bounding box for an area.
     *
     * The lua code must produce a table called 'bbox' that contains a
     * vector of (lat, lon) pairs.
     *
     * @return the Geometry object with the bounding box, or 0 if the
     * computation is unsupported for this area.
     */
    virtual std::unique_ptr<arki::utils::geos::Geometry> operator()(const types::Area& v) const;

    static const BBox& get_singleton();
};

}

#endif
