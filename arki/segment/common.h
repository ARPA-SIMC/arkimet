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
    std::function<void(const std::string&)> reporter;
    const metadata::Collection& mds;
    bool accurate = false;
    size_t end_of_known_data = 0;

    AppendCheckBackend(std::function<void(const std::string&)> reporter, const metadata::Collection& mds);
    virtual ~AppendCheckBackend();

    virtual size_t padding_head(off_t offset, size_t size) const;
    virtual size_t padding_tail(off_t offset, size_t size) const;
    State check_contiguous();

#if 0
    const std::string& gzabspath;
    const std::string& relpath;
    unsigned pad_size = 0;

    CheckBackend(const std::string& gzabspath, const std::string& relpath, std::function<void(const std::string&)> reporter, const metadata::Collection& mds)
        : gzabspath(gzabspath), relpath(relpath), reporter(reporter), mds(mds)
    {
    }

    State check()
    {
        std::unique_ptr<struct stat> st = sys::stat(gzabspath);
        if (st.get() == nullptr)
            return SEGMENT_DELETED;

        // Decompress all the segment in memory
        std::vector<uint8_t> all_data = compress::gunzip(gzabspath);

        if (accurate && !mds.empty())
        {
            const scan::Validator* validator = &scan::Validator::by_format(mds[0].source().format);

            for (const auto& i: mds)
            {
                if (const types::source::Blob* blob = i->has_source_blob())
                {
                    if (blob->filename != relpath)
                        throw std::runtime_error("metadata to validate does not appear to be from this segment");

                    if (blob->offset + blob->size > all_data.size())
                    {
                        reporter("data is supposed to be past the end of the uncompressed segment");
                        return SEGMENT_UNALIGNED;
                    }

                    try {
                        validator->validate_buf(all_data.data() + blob->offset, blob->size);
                    } catch (std::exception& e) {
                        stringstream out;
                        out << "validation failed at " << i->source() << ": " << e.what();
                        reporter(out.str());
                        return SEGMENT_UNALIGNED;
                    }
                } else {
                    try {
                        const auto& buf = i->getData();
                        validator->validate_buf(buf.data(), buf.size());
                    } catch (std::exception& e) {
                        stringstream out;
                        out << "validation failed at " << i->source() << ": " << e.what();
                        reporter(out.str());
                        return SEGMENT_UNALIGNED;
                    }
                }
            }
        }

        // Check for truncation
        off_t file_size = all_data.size();
        if (file_size < end_of_last_data_checked)
        {
            stringstream ss;
            ss << "file looks truncated: its size is " << file_size << " but data is known to exist until " << end_of_last_data_checked << " bytes";
            reporter(ss.str());
            return SEGMENT_UNALIGNED;
        }

        // Check if file_size matches the expected file size
        bool has_hole = false;
        if (file_size > end_of_last_data_checked)
            has_hole = true;

        // Check for holes or elements out of order
        end_of_last_data_checked = 0;
        for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
        {
            const source::Blob& source = (*i)->sourceBlob();
            if (source.offset < (size_t)end_of_last_data_checked || source.offset > (size_t)end_of_last_data_checked) // last padding plus file header
                has_hole = true;

            end_of_last_data_checked = max(end_of_last_data_checked, (off_t)(source.offset + source.size + pad_size));
        }

        // Take note of files with holes
        if (has_hole)
        {
            reporter("segments contains deleted data or needs reordering");
            return SEGMENT_DIRTY;
        } else
            return SEGMENT_OK;
    }
#endif
};

}
}
#endif
