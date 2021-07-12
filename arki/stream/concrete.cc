#include "concrete.h"

namespace arki {
namespace stream {

template class ConcreteStreamOutputBase<ConcreteLinuxBackend>;
template class ConcreteStreamOutputBase<ConcreteTestingBackend>;

}
}

#include "concrete.tcc"
