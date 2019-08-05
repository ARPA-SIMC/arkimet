#include "keys.h"

namespace arki {
namespace emitter {

static Keys make_json()
{
    Keys res;
    res.type_name = "t";
    res.type_desc = "desc";
    res.type_style = "s";
    res.reftime_position_time = "ti";
    res.reftime_period_begin = "b";
    res.reftime_period_end = "e";
    return res;
}

static Keys make_python()
{
    Keys res;
    res.type_name = "type";
    res.type_desc = "desc";
    res.type_style = "style";
    res.reftime_position_time = "time";
    res.reftime_period_begin = "begin";
    res.reftime_period_end = "end";
    return res;
}


const Keys keys_json = make_json();
const Keys keys_python = make_python();

}
}
