#include "concrete.h"
#include "arki/utils/sys.h"
#include "arki/utils/accounting.h"
#include "arki/utils/process.h"
#include "arki/libconfig.h"
#include <system_error>

namespace arki {
namespace stream {

template class ConcreteStreamOutputBase<ConcreteLinuxBackend>;
template class ConcreteStreamOutputBase<ConcreteTestingBackend>;

}
}

#include "concrete.tcc"
