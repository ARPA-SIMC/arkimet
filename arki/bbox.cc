#include "config.h"
#include "arki/bbox.h"
#include "arki/exceptions.h"
#include "arki/utils/geos.h"
#include "arki/utils/string.h"
#include "arki/runtime.h"
#include <memory>

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

}

static std::function<std::unique_ptr<BBox>()> factory;

BBox::~BBox()
{
}


unique_ptr<BBox> BBox::create()
{
    if (factory)
        return factory();
    return unique_ptr<BBox>(new NullBBox);
}

void BBox::set_factory(std::function<std::unique_ptr<BBox>()> new_factory)
{
    factory = new_factory;
}

}
