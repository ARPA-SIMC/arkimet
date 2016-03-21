#include "config.h"
#include <arki/utils/pcounter.h>
#include <arki/utils/sys.h>

namespace arki {
namespace utils {

bool PersistentCounter_fexists__(const std::string& path)
{
    return sys::exists(path);
}

}
}
