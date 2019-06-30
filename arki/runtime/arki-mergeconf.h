#ifndef ARKI_RUNTIME_ARKIMERGECONF_H
#define ARKI_RUNTIME_ARKIMERGECONF_H

#include <arki/runtime/inputs.h>

namespace arki {
namespace runtime {

class ArkiMergeconf
{
public:
    std::vector<std::string> sources;
    bool restrict = false;
    std::string restrict_expr;
    bool ignore_system_ds = false;
    bool extra = false;

    void run(core::cfg::Sections& merged);
};

}
}

#endif
