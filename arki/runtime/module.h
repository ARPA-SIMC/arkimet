#ifndef ARKI_RUNTIME_MODULE_H
#define ARKI_RUNTIME_MODULE_H

namespace arki {
namespace runtime {

struct Module
{
    virtual ~Module() {}

    virtual bool handle_parsed_options() = 0;
};

}
}
#endif
