#include "config.h"

#include <arki/dataset/empty.h>

using namespace std;

namespace arki {
namespace dataset {

Empty::Empty(std::shared_ptr<const Config> config) : m_config(config) {}
Empty::~Empty() {}

}
}
