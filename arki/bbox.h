#ifndef ARKI_BBOX_H
#define ARKI_BBOX_H

#include <arki/types/area.h>
#include <arki/utils/geosfwd.h>
#include <string>
#include <memory>

namespace arki {

/**
 * Compute bounding boxes from type::Area objects
 */
class BBox
{
public:
    virtual ~BBox();

    /**
     * Compute the bounding box for an area.
     */
    virtual std::unique_ptr<arki::utils::geos::Geometry> operator()(const types::Area& v) const = 0;

    static const BBox& get_singleton();
};

}

#endif
