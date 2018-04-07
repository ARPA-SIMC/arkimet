#include "common.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/scan/any.h"
#include "arki/types/source.h"

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

}
}
