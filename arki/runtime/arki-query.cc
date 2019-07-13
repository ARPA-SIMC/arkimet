#include "arki/runtime/arki-query.h"
#include "arki/runtime/processor.h"

namespace arki {
namespace runtime {

ArkiQuery::~ArkiQuery()
{
    delete processor;
}

}
}
