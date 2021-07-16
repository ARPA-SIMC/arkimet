#include "abstract.h"

namespace arki {
namespace stream {

template class AbstractStreamOutput<LinuxBackend>;
template class AbstractStreamOutput<TestingBackend>;

}
}

#include "abstract.tcc"

