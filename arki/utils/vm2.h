#ifndef ARKI_UTILS_VM2_H
#define ARKI_UTILS_VM2_H

#include <arki/values.h>
#include <arki/types/value.h>

#include <meteo-vm2/source.h>

namespace arki {
namespace utils {
namespace vm2 {

std::vector<int> find_stations(const ValueBag& query, meteo::vm2::Source* source=nullptr);
std::vector<int> find_variables(const ValueBag& query, meteo::vm2::Source* source=nullptr);
ValueBag get_station(int id, meteo::vm2::Source* source=nullptr);
ValueBag get_variable(int id, meteo::vm2::Source* source=nullptr);

}
}
}
#endif
