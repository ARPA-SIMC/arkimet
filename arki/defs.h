#ifndef ARKI_DEFS_H
#define ARKI_DEFS_H

/// Core defines common to all arkimet code

#include <arki/libconfig.h>
#include <functional>
#include <memory>

namespace arki {
struct Metadata;

typedef std::function<bool(std::unique_ptr<Metadata>)> metadata_dest_func;


}

#endif
