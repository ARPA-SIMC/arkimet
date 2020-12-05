#ifndef ARKI_UTILS_VM2_H
#define ARKI_UTILS_VM2_H

#include <arki/types/values.h>

namespace meteo {
namespace vm2 {
struct Source;
}
}

namespace arki {
namespace utils {
namespace vm2 {

std::vector<int> find_stations(const types::ValueBag& query, meteo::vm2::Source* source=nullptr);
std::vector<int> find_variables(const types::ValueBag& query, meteo::vm2::Source* source=nullptr);
types::ValueBag get_station(int id, meteo::vm2::Source* source=nullptr);
types::ValueBag get_variable(int id, meteo::vm2::Source* source=nullptr);

}
}
}
#endif
