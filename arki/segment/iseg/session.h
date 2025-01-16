#ifndef ARKI_SEGMENT_ISEG_SESSION_H
#define ARKI_SEGMENT_ISEG_SESSION_H

#include <arki/segment/iseg/fwd.h>
#include <arki/segment/session.h>
#include <arki/types/fwd.h>
#include <string>
#include <set>

namespace arki::segment::iseg {

class Session : public segment::Session
{
public:
    using segment::Session::Session;

    DataFormat format;
    std::set<types::Code> index;
    std::set<types::Code> unique;
    bool trace_sql = false;

    /**
     * If true, try to store the content of small files in the index if
     * possible, to avoid extra I/O when querying
     */
    bool smallfiles = false;

    /**
     * Trade write reliability and write concurrency in favour of performance.
     *
     * Useful for writing fast temporary private datasets.
     */
    bool eatmydata = false;

    std::shared_ptr<arki::Segment> segment_from_relpath_and_format(const std::filesystem::path& relpath, DataFormat format) const override;

    std::shared_ptr<segment::Reader> segment_reader(std::shared_ptr<const arki::Segment> segment, std::shared_ptr<const core::ReadLock> lock) const override;

    std::shared_ptr<RIndex> read_index(const std::filesystem::path& data_relpath, std::shared_ptr<core::ReadLock> lock);
    std::shared_ptr<AIndex> append_index(std::shared_ptr<segment::data::Writer> segment, std::shared_ptr<core::AppendLock> lock);
    std::shared_ptr<CIndex> check_index(const std::filesystem::path& data_relpath, std::shared_ptr<core::CheckLock> lock);
};

}

#endif
