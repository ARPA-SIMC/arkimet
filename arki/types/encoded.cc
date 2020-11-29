#include "encoded.h"

namespace arki {
namespace types {

Encoded::Encoded(const std::vector<uint8_t>& data)
    : data(data) {}

Encoded::Encoded(std::vector<uint8_t>&& data)
    : data(data) {}

Encoded::~Encoded()
{
}


}
}
