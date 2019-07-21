#ifndef ARKI_RUNTIME_H
#define ARKI_RUNTIME_H

#include <memory>

namespace arki {
namespace runtime {
struct Module;

/**
 * Initialise the libraries that we use and parse the matcher alias database.
 */
void init();

}
}
#endif
