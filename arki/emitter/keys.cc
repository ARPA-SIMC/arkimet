#include "keys.h"

namespace arki {
namespace emitter {

static Keys make_json()
{
    Keys res;
    res.type_name = "t";
    res.type_desc = "desc";
    return res;
}

static Keys make_python()
{
    Keys res;
    res.type_name = "type";
    res.type_desc = "desc";
    return res;
}


const Keys keys_json = make_json();
const Keys keys_python = make_python();

}
}
