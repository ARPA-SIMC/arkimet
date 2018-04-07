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
    size_t ofs = 0;
    for (auto& md: mds)
    {
        bool had_cached_data = md->has_cached_data();
        // Read the data
        auto buf = md->getData();
        // Validate it if requested
        if (validator)
            validator->validate_buf(buf.data(), buf.size());
        size_t appended = append(buf);
        auto new_source = types::Source::createBlobUnlocked(md->source().format, root, relpath, ofs, buf.size());
        // Update the source information in the metadata
        md->set_source(std::move(new_source));
        // Drop the cached data, to prevent accidentally loading the whole segment in memory
        if (!had_cached_data) md->drop_cached_data();
        ofs += appended;
    }
}

AppendCheckBackend::AppendCheckBackend(std::function<void(const std::string&)> reporter, const metadata::Collection& mds)
    : reporter(reporter), mds(mds)
{
}

AppendCheckBackend::~AppendCheckBackend()
{
}

size_t AppendCheckBackend::compute_padding(off_t offset, size_t size) const
{
    return 0;
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
        // If an item begins after the end of another, they overlap and the file needs rescanning
        if (i.offset < end_of_known_data)
        {
            stringstream out;
            out << "item at offset " << i.offset << " overlaps with the previous items that ends at offset" << end_of_known_data;
            reporter(out.str());
            return SEGMENT_UNALIGNED;
        }
        else if (!dirty && i.offset > end_of_known_data)
        {
            stringstream out;
            out << "item at offset " << i.offset << " begins past the end of the previous item (offset " << end_of_known_data << ")";
            reporter(out.str());
            dirty = true;
        }

        end_of_known_data = i.offset + i.size + compute_padding(i.offset, i.size);
    }

    return dirty ? SEGMENT_DIRTY : SEGMENT_OK;
}

}
}
