#ifndef ARKI_SEGMENT_ISEG_INDEX_BASE_H
#define ARKI_SEGMENT_ISEG_INDEX_BASE_H

#include <string>
#include <vector>

namespace arki::segment::iseg::index {

// Exception we can throw if an element was not found
struct NotFound
{
};

/**
 * Format a vector of ints to a SQL match expression.
 *
 * If the vector has only one item, it becomes =item.
 *
 * If the vector has more than one item, it becomes IN(val1,val2...)
 */
std::string fmtin(const std::vector<int>& vals);

} // namespace arki::segment::iseg::index
#endif
