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

#include <arki/utils/vm2.h>
#include <arki/types/value.h>
#include <arki/runtime/config.h>

namespace arki {
namespace utils {
namespace vm2 {

Item<types::Value> decodeMdValue(const ::vm2::Value& value) {
  std::stringstream mdvalue;
  if (value.value1 != ::vm2::MISSING_DOUBLE) {
    mdvalue << value.value1;
  }
  mdvalue << ",";
  if (value.value2 != ::vm2::MISSING_DOUBLE) {
    mdvalue << value.value2;
  }
  mdvalue << "," << value.value3 << "," << value.flags;
  return types::Value::create(mdvalue.str());
}

Source* Source::_instance = NULL;

Source& Source::get() {
  if (!_instance) {
    std::string s = runtime::Config::get().file_vm2_config;
    if (!s.empty())
      _instance = new Vm2Source(s);
    else
      _instance = new DummySource;
  }
  return *_instance;
}

Vm2Source::Vm2Source(const std::string& path) : source(path) {
  L = source.L;
}
Vm2Source::~Vm2Source() {}

std::vector<int> Vm2Source::find_stations(const ValueBag& query) {
  std::vector<int> res;
  query.lua_push(L);
  int idx = lua_gettop(L);
  try {
    res = source.lua_find_stations(idx);
  } catch (const std::exception& e) {
    lua_pop(L,1);
    throw;
  }
  lua_pop(L,1);
  return res;
}
std::vector<int> Vm2Source::find_variables(const ValueBag& query) {
  std::vector<int> res;
  query.lua_push(L);
  int idx = lua_gettop(L);
  try {
    res = source.lua_find_variables(idx);
  } catch (const std::exception& e) {
    lua_pop(L,1);
    throw;
  }
  lua_pop(L,1);
  return res;
}
ValueBag Vm2Source::get_station(int id) {
  ValueBag vb;
  source.lua_push_station(id);
  if (lua_istable(L, -1)) {
    vb.load_lua_table(L, -1);
  } else {
    lua_pop(L,1);
  }
  return vb;
}
ValueBag Vm2Source::get_variable(int id) {
  ValueBag vb;
  source.lua_push_variable(id);
  if (lua_istable(L, -1)) {
    vb.load_lua_table(L, -1);
  } else {
    lua_pop(L,1);
  }
  return vb;
}

}
}
}
