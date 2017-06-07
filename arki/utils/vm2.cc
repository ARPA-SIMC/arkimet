/*
 * utils/vm2 - VM2 helpers
 *
 * Copyright (C) 2012  ARPAE-SIMC <simc-urp@arpae.it>
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

#include <meteo-vm2/source.h>

namespace arki {
namespace utils {
namespace vm2 {

std::vector<int> find_stations(const ValueBag& query)
{
  meteo::vm2::Source* source = meteo::vm2::Source::get();
  lua_State* L = source->L;

  std::vector<int> res;
  query.lua_push(L);
  int idx = lua_gettop(L);
  try {
    res = source->lua_find_stations(idx);
  } catch (const std::exception& e) {
    lua_pop(L,1);
    throw;
  }
  lua_pop(L,1);
  return res;
}
std::vector<int> find_variables(const ValueBag& query)
{
  meteo::vm2::Source* source = meteo::vm2::Source::get();
  lua_State* L = source->L;

  std::vector<int> res;
  query.lua_push(L);
  int idx = lua_gettop(L);
  try {
    res = source->lua_find_variables(idx);
  } catch (const std::exception& e) {
    lua_pop(L,1);
    throw;
  }
  lua_pop(L,1);
  return res;
}
ValueBag get_station(int id)
{
  meteo::vm2::Source* source = meteo::vm2::Source::get();
  lua_State* L = source->L;

  ValueBag vb;
  source->lua_push_station(id);
  if (lua_istable(L, -1)) {
    vb.load_lua_table(L, -1);
  } else {
    lua_pop(L,1);
  }
  return vb;
}
ValueBag get_variable(int id)
{
  meteo::vm2::Source* source = meteo::vm2::Source::get();
  lua_State* L = source->L;

  ValueBag vb;
  source->lua_push_variable(id);
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
