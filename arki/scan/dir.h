#ifndef ARKI_SCAN_DIR_H
#define ARKI_SCAN_DIR_H

#include <string>
#include <set>

namespace arki {
namespace scan {

/**
 * Scan a dataset for data files, returning a set of pathnames relative to
 * root.
 */
std::set<std::string> dir(const std::string& root, bool files_in_root=false);

}
}

#endif
