#include "common.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/metadata/collection.h"
#include "arki/scan/validator.h"
#include "arki/types/source.h"
#include "arki/types/source/blob.h"
#include <algorithm>
#include <sstream>

using namespace std;

namespace arki {
namespace segment {

AppendCreator::AppendCreator(const Segment& segment, metadata::Collection& mds)
    : segment(segment), mds(mds)
{
}

AppendCreator::~AppendCreator()
{
}

size_t AppendCreator::append(const metadata::Data& data)
{
    throw std::runtime_error("append of buffers not implemented for this segment");
}

Span AppendCreator::append_md(Metadata& md)
{
    // Read the data
    const auto& data = md.get_data();
    // Validate it if requested
    if (validator)
        validator->validate_data(data);
    return Span(append(data), data.size());
}

std::unique_ptr<types::Source> AppendCreator::create_source(const Metadata& md, const Span& span)
{
    return types::Source::createBlobUnlocked(md.source().format, segment.root, segment.relpath, span.offset, span.size);
}

void AppendCreator::create()
{
    // Fill the temp file with all the data in the right order
    for (auto& md: mds)
    {
        bool had_cached_data = md->has_cached_data();
        Span span = append_md(*md);
        // Update the source information in the metadata
        md->set_source(create_source(*md, span));
        // Drop the cached data, to prevent accidentally loading the whole segment in memory
        if (!had_cached_data) md->drop_cached_data();
    }
}


AppendCheckBackend::AppendCheckBackend(std::function<void(const std::string&)> reporter, const std::filesystem::path& relpath, const metadata::Collection& mds)
    : relpath(relpath), reporter(reporter), mds(mds)
{
}

AppendCheckBackend::~AppendCheckBackend()
{
}

size_t AppendCheckBackend::actual_start(off_t offset, size_t size) const
{
    return offset;
}

size_t AppendCheckBackend::actual_end(off_t offset, size_t size) const
{
    return offset + size;
}

size_t AppendCheckBackend::compute_unindexed_space(const std::vector<Span>& indexed_spans) const
{
    size_t res = offset_end();
    for (const auto& i: indexed_spans)
        res -= i.size;
    return res;
}

#if 0
void AppendCheckBackend::validate(Metadata& md, const types::source::Blob& source) const
{
    auto buf = md.getData();
    validator->validate_buf(buf.data(), buf.size());
}
#endif

State AppendCheckBackend::check_source(const types::source::Blob& source)
{
    if (source.filename != relpath)
        throw std::runtime_error("metadata to validate does not appear to be from this segment");
    return State(SEGMENT_OK);
}

State AppendCheckBackend::check_contiguous()
{
    bool dirty = false;

    // List of data (offset, size) sorted by offset, to detect overlaps
    std::vector<Span> spans;
    for (const auto& md: mds)
    {
        const types::source::Blob& source = md->sourceBlob();
        State state = check_source(source);
        if (!state.is_ok())
            return state;

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
        size_t start = actual_start(i.offset, i.size);

        // If an item begins after the end of another, they overlap and the file needs rescanning
        if (start < end_of_known_data)
        {
            stringstream out;
            out << "item at offset " << start << " overlaps with the previous items that ends at offset " << end_of_known_data;
            reporter(out.str());
            return State(SEGMENT_CORRUPTED);
        }
        else if (!dirty && start > end_of_known_data)
        {
            stringstream out;
            out << "item at offset " << start << " begins past the end of the previous item (offset " << end_of_known_data << ")";
            reporter(out.str());
            dirty = true;
        }

        end_of_known_data = actual_end(i.offset, i.size);
    }

    // Report estimates of repack savings (see #187)
    size_t unindexed_size = compute_unindexed_space(spans);
    if (unindexed_size > 0)
    {
        std::stringstream out;
        out << "deleted/duplicated/replaced data found: " << unindexed_size << "b would be freed by a repack";
        reporter(out.str());
    }

    // Check the match between end of data and end of file
    size_t end = offset_end();
    if (end < end_of_known_data)
    {
        stringstream ss;
        ss << "file looks truncated: data ends at offset " << end << " but it is supposed to extend until " << end_of_known_data << " bytes";
        reporter(ss.str());
        return State(SEGMENT_CORRUPTED);
    }

    if (!dirty && end > end_of_known_data)
    {
        reporter("segment contains deleted data at the end");
        dirty = true;
    }

    return State(dirty ? SEGMENT_DIRTY : SEGMENT_OK);
}

State AppendCheckBackend::validate_data()
{
    if (mds.empty())
        return State(SEGMENT_OK);

    validator = &scan::Validator::by_format(mds[0].source().format);
    size_t end = offset_end();

    for (const auto& md: mds)
    {
        const types::source::Blob& source = md->sourceBlob();

        if (actual_end(source.offset, source.size) > end)
        {
            reporter("data at offset " + std::to_string(source.offset) + " would continue past the end of the segment");
            return State(SEGMENT_CORRUPTED);
        }

        try {
            validate(*md, source);
        } catch (std::exception& e) {
            stringstream out;
            out << "validation failed at " << md->source() << ": " << e.what();
            reporter(out.str());
            return State(SEGMENT_CORRUPTED);
        }
    }

    return State(SEGMENT_OK);
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

    return State(SEGMENT_OK);
}

}
}
