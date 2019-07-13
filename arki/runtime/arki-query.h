#ifndef ARKI_RUNTIME_ARKIQUERY_H
#define ARKI_RUNTIME_ARKIQUERY_H

#include <arki/core/cfg.h>
#include <arki/runtime/processor.h>

namespace arki {
namespace runtime {

class ArkiQuery
{
public:
    core::cfg::Sections inputs;
    DatasetProcessor* processor = nullptr;

    ArkiQuery() = default;
    ~ArkiQuery();
    ArkiQuery(const ArkiQuery&) = delete;
    ArkiQuery(ArkiQuery&&) = delete;
    ArkiQuery& operator=(const ArkiQuery&) = delete;
    ArkiQuery& operator=(ArkiQuery&&) = delete;
};

}
}

#endif
