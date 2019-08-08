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
    virtual std::unique_ptr<arki::utils::geos::Geometry> compute(const types::Area& v) const = 0;

    static std::unique_ptr<BBox> create();
    static void set_factory(std::function<std::unique_ptr<BBox>()> new_factory);
};

}

#endif
