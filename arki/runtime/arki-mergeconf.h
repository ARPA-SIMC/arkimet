#ifndef ARKI_RUNTIME_ARKIMERGECONF_H
#define ARKI_RUNTIME_ARKIMERGECONF_H

#include <arki/runtime/inputs.h>

namespace arki {
namespace runtime {

class ArkiMergeconf
{
public:
    bool extra = false;

    void run(core::cfg::Sections& merged);
};

}
}

#endif
