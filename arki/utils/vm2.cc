#include <arki/utils/vm2.h>
#include <meteo-vm2/source.h>
#include <arki/types/value.h>

namespace arki {
namespace utils {
namespace vm2 {

std::vector<int> find_stations(const ValueBag& query, meteo::vm2::Source* s)
{
  meteo::vm2::Source* source = (s == nullptr ? meteo::vm2::Source::get() : s);
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

std::vector<int> find_variables(const ValueBag& query, meteo::vm2::Source* s)
{
  meteo::vm2::Source* source = (s == nullptr ? meteo::vm2::Source::get() : s);
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

ValueBag get_station(int id, meteo::vm2::Source* s)
{
  meteo::vm2::Source* source = (s == nullptr ? meteo::vm2::Source::get() : s);
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

ValueBag get_variable(int id, meteo::vm2::Source* s)
{
  meteo::vm2::Source* source = (s == nullptr ? meteo::vm2::Source::get() : s);
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
