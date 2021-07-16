#include "concrete.h"

namespace arki {
namespace stream {

template class ConcreteStreamOutputBase<LinuxBackend>;
template class ConcreteStreamOutputBase<TestingBackend>;

}
}

#include "concrete.tcc"
