#include "iseg.h"
#include "iseg/reader.h"
#include "iseg/writer.h"
#include "iseg/checker.h"
#include "lock.h"
#include "arki/defs.h"
#include "arki/types.h"
#include "arki/segment/iseg/session.h"
#include "arki/dataset/session.h"
#include "arki/utils/string.h"

using namespace std;
using namespace arki::utils;

namespace arki::dataset::iseg {

Dataset::Dataset(std::shared_ptr<Session> session, const core::cfg::Section& cfg)
    : segmented::Dataset(session, std::make_shared<segment::iseg::Session>(cfg.value("path")), cfg),
      iseg_segment_session(std::static_pointer_cast<segment::iseg::Session>(segment_session)),
      summary_cache_pathname{path / ".summaries"}
{
    iseg_segment_session->format = format_from_string(cfg.value("format"));
    iseg_segment_session->index = types::parse_code_names(cfg.value("index"));
    iseg_segment_session->unique = types::parse_code_names(cfg.value("unique"));
    iseg_segment_session->trace_sql = cfg.value_bool("trace_sql");
    iseg_segment_session->smallfiles = smallfiles;
    iseg_segment_session->eatmydata = eatmydata;
    iseg_segment_session->unique.erase(TYPE_REFTIME);
}

std::shared_ptr<dataset::Reader> Dataset::create_reader()
{
    return std::make_shared<iseg::Reader>(static_pointer_cast<Dataset>(shared_from_this()));
}

std::shared_ptr<dataset::Writer> Dataset::create_writer()
{
    return std::make_shared<iseg::Writer>(static_pointer_cast<Dataset>(shared_from_this()));
}

std::shared_ptr<dataset::Checker> Dataset::create_checker()
{
    return std::make_shared<iseg::Checker>(static_pointer_cast<Dataset>(shared_from_this()));
}

std::shared_ptr<core::ReadLock> Dataset::read_lock_segment(const std::filesystem::path& relpath) const
{
    return std::make_shared<SegmentReadLock>(*this, relpath);
}

std::shared_ptr<core::AppendLock> Dataset::append_lock_segment(const std::filesystem::path& relpath) const
{
    return std::make_shared<SegmentAppendLock>(*this, relpath);
}

std::shared_ptr<core::CheckLock> Dataset::check_lock_segment(const std::filesystem::path& relpath) const
{
    return std::make_shared<SegmentCheckLock>(*this, relpath);
}

}
