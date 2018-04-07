#include "common.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/scan/any.h"
#include "arki/types/source.h"
#include "arki/types/source/blob.h"
#include <algorithm>
#include <sstream>

using namespace std;

namespace arki {
namespace segment {

AppendCreator::AppendCreator(const std::string& root, const std::string& relpath, const std::string& abspath, metadata::Collection& mds)
    : root(root), relpath(relpath), abspath(abspath), mds(mds)
{
}

AppendCreator::~AppendCreator()
{
}

void AppendCreator::create()
{
    // Fill the temp file with all the data in the right order
    for (auto& md: mds)
    {
        bool had_cached_data = md->has_cached_data();
        // Read the data
        auto buf = md->getData();
        // Validate it if requested
        if (validator)
            validator->validate_buf(buf.data(), buf.size());
        size_t ofs = append(buf);
        auto new_source = types::Source::createBlobUnlocked(md->source().format, root, relpath, ofs, buf.size());
        // Update the source information in the metadata
        md->set_source(std::move(new_source));
        // Drop the cached data, to prevent accidentally loading the whole segment in memory
        if (!had_cached_data) md->drop_cached_data();
    }
}

AppendCheckBackend::AppendCheckBackend(std::function<void(const std::string&)> reporter, const std::string& relpath, const metadata::Collection& mds)
    : relpath(relpath), reporter(reporter), mds(mds)
{
}

AppendCheckBackend::~AppendCheckBackend()
{
}

size_t AppendCheckBackend::padding_head(off_t offset, size_t size) const
{
    return 0;
}

size_t AppendCheckBackend::padding_tail(off_t offset, size_t size) const
{
    return 0;
}

void AppendCheckBackend::validate(Metadata& md, const types::source::Blob& source) const
{
    auto buf = md.getData();
    validator->validate_buf(buf.data(), buf.size());
}

namespace {
struct Span
{
    size_t offset;
    size_t size;
    Span(size_t offset, size_t size) : offset(offset), size(size) {}
    bool operator<(const Span& o) const { return std::tie(offset, size) < std::tie(o.offset, o.size); }
};
}

State AppendCheckBackend::check_contiguous()
{
    bool dirty = false;

    // List of data (offset, size) sorted by offset, to detect overlaps
    std::vector<Span> spans;
    for (const auto& md: mds)
    {
        const types::source::Blob& source = md->sourceBlob();
        if (source.filename != relpath)
            throw std::runtime_error("metadata to validate does not appear to be from this segment");

        if (!dirty && !spans.empty() && source.offset < spans.back().offset)
        {
            stringstream out;
            out << "item at offset " << source.offset << " is wrongly ordered before item at offset " << spans.back().offset;
            reporter(out.str());
            dirty = true;
        }
        spans.push_back(Span(source.offset, source.size));
    }
    std::sort(spans.begin(), spans.end());

    // Check for overlaps
    for (const auto& i: spans)
    {
        size_t start = i.offset - padding_head(i.offset, i.size);

        // If an item begins after the end of another, they overlap and the file needs rescanning
        if (start < end_of_known_data)
        {
            stringstream out;
            out << "item at offset " << start << " overlaps with the previous items that ends at offset" << end_of_known_data;
            reporter(out.str());
            return SEGMENT_UNALIGNED;
        }
        else if (!dirty && start > end_of_known_data)
        {
            stringstream out;
            out << "item at offset " << start << " begins past the end of the previous item (offset " << end_of_known_data << ")";
            reporter(out.str());
            dirty = true;
        }

        end_of_known_data = i.offset + i.size + padding_tail(i.offset, i.size);
    }

    // Check the match between end of data and end of file
    size_t end = offset_end();
    if (end < end_of_known_data)
    {
        stringstream ss;
        ss << "file looks truncated: data end at offset " << end << " but it is supposed to extend until " << end_of_known_data << " bytes";
        reporter(ss.str());
        return SEGMENT_UNALIGNED;
    }

    if (!dirty && end > end_of_known_data)
    {
        reporter("segment contains possibly deleted data at the end");
        dirty = true;
    }

    return dirty ? SEGMENT_DIRTY : SEGMENT_OK;
}

State AppendCheckBackend::validate_data()
{
    if (mds.empty())
        return SEGMENT_OK;

    validator = &scan::Validator::by_format(mds[0].source().format);
    size_t end = offset_end();

    for (const auto& md: mds)
    {
        const types::source::Blob& source = md->sourceBlob();

        if (source.offset + source.size > end)
        {
            reporter("data is supposed to be past the end of the uncompressed segment");
            return SEGMENT_UNALIGNED;
        }

        try {
            validate(*md, source);
        } catch (std::exception& e) {
            stringstream out;
            out << "validation failed at " << md->source() << ": " << e.what();
            reporter(out.str());
            return SEGMENT_UNALIGNED;
        }
    }

    return SEGMENT_OK;
}

State AppendCheckBackend::check()
{
    if (accurate)
    {
        State state = validate_data();
        if (!state.is_ok())
            return state;
    }

    State state = check_contiguous();
    if (!state.is_ok())
        return state;

    return SEGMENT_OK;
}

}
}
