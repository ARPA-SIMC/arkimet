#ifndef ARKI_SEGMENT_COMMON_H
#define ARKI_SEGMENT_COMMON_H

#include <arki/segment.h>
#include <arki/metadata/fwd.h>
#include <arki/scan/fwd.h>
#include <string>
#include <vector>
#include <functional>

namespace arki {
namespace segment {

struct Span
{
    size_t offset;
    size_t size;
    Span() = default;
    Span(size_t offset, size_t size) : offset(offset), size(size) {}
    bool operator<(const Span& o) const { return std::tie(offset, size) < std::tie(o.offset, o.size); }
};

struct AppendCreator
{
    const std::string& root;
    const std::string& relpath;
    metadata::Collection& mds;
    const scan::Validator* validator = nullptr;

    AppendCreator(const std::string& root, const std::string& relpath, metadata::Collection& mds);
    virtual ~AppendCreator();

    /// Append data to the segment, returning the offset at which it has been written
    virtual Span append_md(Metadata& md);

    /// Append data to the segment, returning the offset at which it has been written
    virtual size_t append(const std::vector<uint8_t>& data);

    /// Perform segment creation
    void create();
};

struct AppendCheckBackend
{
    const std::string& relpath;
    std::function<void(const std::string&)> reporter;
    const metadata::Collection& mds;
    bool accurate = false;
    size_t end_of_known_data = 0;
    const scan::Validator* validator = nullptr;

    AppendCheckBackend(std::function<void(const std::string&)> reporter, const std::string& relpath, const metadata::Collection& mds);
    virtual ~AppendCheckBackend();

    virtual void validate(Metadata& md, const types::source::Blob& source) = 0;
    virtual size_t offset_end() const = 0;
    virtual size_t actual_start(off_t offset, size_t size) const;
    virtual size_t actual_end(off_t offset, size_t size) const;
    virtual State check_source(const types::source::Blob& source);
    State check_contiguous();
    State validate_data();
    State check();
};

}
}
#endif
