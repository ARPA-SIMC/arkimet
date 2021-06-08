#include "concrete-timeout.h"
#include "arki/utils/sys.h"
#include "arki/utils/accounting.h"
#include "arki/utils/process.h"
#include "arki/libconfig.h"
#include <system_error>
#include <sys/uio.h>
#include <sys/sendfile.h>

namespace arki {
namespace stream {

template class ConcreteTimeoutStreamOutputBase<ConcreteLinuxBackend>;
template class ConcreteTimeoutStreamOutputBase<ConcreteTestingBackend>;

}
}

#include "concrete-timeout.tcc"
