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

    explicit Session(const core::cfg::Section& cfg);

    std::shared_ptr<arki::Segment> segment_from_relpath_and_format(const std::filesystem::path& relpath, DataFormat format) const override;

    std::shared_ptr<segment::Reader> segment_reader(std::shared_ptr<const arki::Segment> segment, std::shared_ptr<const core::ReadLock> lock) const override;
    std::shared_ptr<segment::Checker> segment_checker(std::shared_ptr<const arki::Segment> segment, std::shared_ptr<core::CheckLock> lock) const override;

    std::shared_ptr<AIndex> append_index(std::shared_ptr<const arki::Segment> segment, std::shared_ptr<core::AppendLock> lock) const;

    void create_iseg(std::shared_ptr<arki::Segment> segment, arki::metadata::Collection& mds) const override;
};

}

#endif
