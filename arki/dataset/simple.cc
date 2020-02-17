#include "simple.h"
#include "simple/reader.h"
#include "simple/writer.h"
#include "simple/checker.h"
#include "step.h"
#include "arki/utils/string.h"
#include "arki/metadata.h"
#include "arki/types/reftime.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace simple {

Dataset::Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : dataset::indexed::Dataset(session, cfg),
      index_type(cfg.value("index_type"))
{
}

std::unique_ptr<dataset::Reader> Dataset::create_reader() const
{
    auto cfg = dynamic_pointer_cast<const simple::Dataset>(shared_from_this());
    return std::unique_ptr<dataset::Reader>(new simple::Reader(cfg));
}

std::unique_ptr<dataset::Writer> Dataset::create_writer() const
{
    auto cfg = dynamic_pointer_cast<const simple::Dataset>(shared_from_this());
    return std::unique_ptr<dataset::Writer>(new simple::Writer(cfg));
}

std::unique_ptr<dataset::Checker> Dataset::create_checker() const
{
    auto cfg = dynamic_pointer_cast<const simple::Dataset>(shared_from_this());
    return std::unique_ptr<dataset::Checker>(new simple::Checker(cfg));
}

}
}
}

