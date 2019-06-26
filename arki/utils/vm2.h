#ifndef ARKI_UTILS_VM2_H
#define ARKI_UTILS_VM2_H

#include <arki/values.h>
#include <arki/types/value.h>

namespace arki {
namespace utils {
namespace vm2 {

std::vector<int> find_stations(const ValueBag& query);
std::vector<int> find_variables(const ValueBag& query);
ValueBag get_station(int id);
ValueBag get_variable(int id);

}
}
}
#endif
