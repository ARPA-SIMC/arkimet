#ifndef ARKI_SCAN_ANY_H
#define ARKI_SCAN_ANY_H

/// Format-independent metadata extraction and validation

#include <arki/libconfig.h>
#include <arki/defs.h>
#include <arki/core/fwd.h>
#include <arki/types/fwd.h>
#include <vector>
#include <string>
#include <ctime>
#include <sys/types.h>

namespace arki {
class Metadata;

namespace scan {

/**
 * Return the file mtime, whether it is compressed or not
 */
time_t timestamp(const std::string& file);

/**
 * Reconstruct raw data based on a metadata and a value
 */
std::vector<uint8_t> reconstruct(const std::string& format, const Metadata& md, const std::string& value);

}
}
#endif
