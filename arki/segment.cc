#include "segment.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/nag.h"
#include "arki/query.h"
#include "arki/stream.h"
#include "arki/summary.h"
#include "arki/types/source/blob.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "segment/data/dir.h"
#include "segment/data/fd.h"
#include "segment/data/gz.h"
#include "segment/data/missing.h"
#include "segment/data/tar.h"
#include "segment/data/zip.h"
#include "segment/metadata.h"
#include <memory>

using namespace std::string_literals;
using namespace arki::utils;

namespace arki {

Segment::Segment(std::shared_ptr<const segment::Session> session,
                 DataFormat format, const std::filesystem::path& relpath)
    : m_session(session), m_format(format),
      m_abspath(sys::abspath(session->root / relpath))
{
    m_relpath = std::filesystem::relative(m_abspath, root());
    if (m_relpath.begin() == m_relpath.end())
        throw std::runtime_error("relative segment path is empty");
    auto lead = *m_relpath.begin();
    if (lead == "..")
        throw std::runtime_error(
            "relative segment path points outside the segment root");
    if (lead == ".")
        throw std::runtime_error("relative segment path is empty");
}

Segment::~Segment() {}

std::filesystem::path Segment::abspath_metadata() const
{
    return sys::with_suffix(m_abspath, ".metadata");
}

std::filesystem::path Segment::abspath_summary() const
{
    return sys::with_suffix(m_abspath, ".summary");
}

std::filesystem::path Segment::abspath_iseg_index() const
{
    return sys::with_suffix(m_abspath, ".index");
}

std::shared_ptr<segment::Reader>
Segment::reader(std::shared_ptr<const core::ReadLock> lock) const
{
    return session().segment_reader(shared_from_this(), lock);
}

std::shared_ptr<segment::Writer>
Segment::writer(std::shared_ptr<core::AppendLock> lock) const
{
    return session().segment_writer(shared_from_this(), lock);
}

std::shared_ptr<segment::Checker>
Segment::checker(std::shared_ptr<core::CheckLock> lock) const
{
    return session().segment_checker(shared_from_this(), lock);
}

std::filesystem::path Segment::basename(const std::filesystem::path& pathname)
{
    const auto& native = pathname.native();
    if (str::endswith(native, ".gz"))
        return native.substr(0, native.size() - 3);
    if (str::endswith(native, ".tar"))
        return native.substr(0, native.size() - 4);
    if (str::endswith(native, ".zip"))
        return native.substr(0, native.size() - 4);
    return pathname;
}

namespace segment {

/*
 * Reader
 */

Reader::Reader(std::shared_ptr<const Segment> segment,
               std::shared_ptr<const core::ReadLock> lock)
    : m_segment(segment), lock(lock)
{
}

Reader::~Reader() {}

#if 0
void Reader::query_summary(const Matcher& matcher, Summary& summary)
{
    query_data(query::Data(matcher), [&](std::shared_ptr<Metadata> md) { summary.add(*md); return true; });
}
#endif

/*
 * EmptyReader
 */

bool EmptyReader::has_changed() const
{
    // Always return true, so that at each reader instantiation in the seession
    // we recheck if the segment has appeared.
    // It's ok not to aggressively reuse empty readers, as they hold no
    // resources
    return true;
}

std::vector<uint8_t> EmptyReader::read(const types::source::Blob&)
{
    throw std::runtime_error("Cannot read Blob data from an empty reader");
}
stream::SendResult EmptyReader::stream(const types::source::Blob&,
                                       StreamOutput&)
{
    throw std::runtime_error("Cannot stream Blob data from an empty reader");
}

bool EmptyReader::read_all(metadata_dest_func) { return true; }

bool EmptyReader::query_data(const query::Data&, metadata_dest_func)
{
    return true;
}

void EmptyReader::query_summary(const Matcher&, Summary&) {}

/*
 * Writer
 */

Writer::Writer(std::shared_ptr<const Segment> segment,
               std::shared_ptr<core::AppendLock> lock)
    : m_segment(segment), lock(lock)
{
}

Writer::~Writer() {}

Checker::Checker(std::shared_ptr<const Segment> segment,
                 std::shared_ptr<core::CheckLock> lock)
    : lock(lock), m_segment(segment)
{
}

Checker::~Checker() {}

/*
 * RepackConfig
 */

RepackConfig::RepackConfig() {}
RepackConfig::RepackConfig(unsigned gz_group_size, unsigned test_flags)
    : gz_group_size(gz_group_size), test_flags(test_flags)
{
}

/*
 * Fixer
 */

Fixer::Fixer(std::shared_ptr<Checker> checker,
             std::shared_ptr<core::CheckWriteLock> lock)
    : lock(lock), m_checker(checker)
{
}

Fixer::~Fixer() {}

size_t Fixer::remove_ifexists(const std::filesystem::path& path)
{
    size_t res = 0;
    if (std::filesystem::exists(path))
    {
        res = sys::size(path);
        sys::unlink(path);
    }
    return res;
}

} // namespace segment

} // namespace arki
