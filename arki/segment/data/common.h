#ifndef ARKI_SEGMENT_COMMON_H
#define ARKI_SEGMENT_COMMON_H

#include <arki/segment/data.h>
#include <arki/types/fwd.h>
#include <arki/metadata/fwd.h>
#include <arki/scan/fwd.h>
#include <string>
#include <functional>

namespace arki {
namespace segment {

struct AppendCreator
{
    const Segment& segment;
    arki::metadata::Collection& mds;
    const arki::scan::Validator* validator = nullptr;

    AppendCreator(const Segment& segment, arki::metadata::Collection& mds);
    virtual ~AppendCreator();

    /// Create a source for md as imported into this segment at the given Span
    virtual std::unique_ptr<types::Source> create_source(const Metadata& md, const Span& span);

    /// Append data to the segment, returning the offset at which it has been written
    virtual Span append_md(Metadata& md);

    /// Append data to the segment, returning the offset at which it has been written
    virtual size_t append(const arki::metadata::Data& data);

    /// Perform segment creation
    void create();
};

struct AppendCheckBackend
{
    const Segment& segment;
    std::function<void(const std::string&)> reporter;
    const arki::metadata::Collection& mds;
    bool accurate = false;
    size_t end_of_known_data = 0;
    const arki::scan::Validator* validator = nullptr;

    AppendCheckBackend(std::function<void(const std::string&)> reporter, const Segment& segment, const arki::metadata::Collection& mds);
    virtual ~AppendCheckBackend();

    virtual void validate(Metadata& md, const types::source::Blob& source) = 0;
    virtual size_t offset_end() const = 0;
    virtual size_t actual_start(off_t offset, size_t size) const;
    virtual size_t actual_end(off_t offset, size_t size) const;
    virtual size_t compute_unindexed_space(const std::vector<Span>& indexed_spans) const;
    virtual State check_source(const types::source::Blob& source);
    State check_contiguous();
    State validate_data();
    State check();
};

}
}
#endif
