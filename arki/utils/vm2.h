/*
 * utils/vm2 - VM2 helpers
 *
 * Copyright (C) 2012  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Emanuele Di Giacomo <edigiacomo@arpa.emr.it>
 */
#ifndef ARKI_UTILS_VM2_H
#define ARKI_UTILS_VM2_H

#include <arki/values.h>
#include <arki/types/value.h>

#include <vm2/value.h>
#include <vm2/source.h>

namespace arki {
namespace utils {
namespace vm2 {

Item<types::Value> decodeMdValue(const ::vm2::Value& value);

class Source {
 private:
  static Source* _instance;

 public:
  static Source& get();

  virtual ~Source() {}
  virtual std::vector<int> find_stations(const ValueBag& query) = 0;
  virtual std::vector<int> find_variables(const ValueBag& query) = 0;
  virtual ValueBag get_station(int id) = 0;
  virtual ValueBag get_variable(int id) = 0;
};

struct DummySource : public Source {
  virtual ~DummySource() {}
  virtual std::vector<int> find_stations(const ValueBag& query) { return std::vector<int>(); }
  virtual std::vector<int> find_variables(const ValueBag& query){ return std::vector<int>(); }
  virtual ValueBag get_station(int id) { return ValueBag(); }
  virtual ValueBag get_variable(int id) { return ValueBag(); }
};

struct Vm2Source : public Source {
  lua_State *L;
  ::vm2::Source source;

  Vm2Source(const std::string& path);
  virtual ~Vm2Source();

  virtual std::vector<int> find_stations(const ValueBag& query);
  virtual std::vector<int> find_variables(const ValueBag& query);
  virtual ValueBag get_station(int id);
  virtual ValueBag get_variable(int id);
};

}
}
}
#endif        /* ARKI_UTILS_VM2_H */
