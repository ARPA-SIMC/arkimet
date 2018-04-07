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

struct AppendCreator
{
    const std::string& root;
    const std::string& relpath;
    const std::string& abspath;
    metadata::Collection& mds;
    const scan::Validator* validator = nullptr;

    AppendCreator(const std::string& root, const std::string& relpath, const std::string& abspath, metadata::Collection& mds);
    virtual ~AppendCreator();

    /// Append data to the segment, returning the offset at which it has been written
    virtual size_t append(const std::vector<uint8_t>& data) = 0;

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

    virtual void validate(Metadata& md, const types::source::Blob& source) const;
    virtual size_t offset_end() const = 0;
    virtual size_t padding_head(off_t offset, size_t size) const;
    virtual size_t padding_tail(off_t offset, size_t size) const;
    State check_contiguous();
    State validate_data();
    State check();
};

}
}
#endif
