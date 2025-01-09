#ifndef ARKI_SEGMENT_BASE_TCC
#define ARKI_SEGMENT_BASE_TCC

#include "arki/segment/data/base.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <sstream>

namespace arki::segment::data {

template<typename Data>
std::shared_ptr<data::Checker> BaseChecker<Data>::move(const std::filesystem::path& new_root, const std::filesystem::path& new_relpath, const std::filesystem::path& new_abspath)
{
    using namespace arki::utils;

    // Sanity checks: avoid conflicts
    if (std::filesystem::exists(new_abspath) ||
            std::filesystem::exists(sys::with_suffix(new_abspath, ".tar")) ||
            std::filesystem::exists(sys::with_suffix(new_abspath, ".gz")) ||
            std::filesystem::exists(sys::with_suffix(new_abspath, ".zip")))
    {
        std::stringstream ss;
        ss << "cannot archive " << this->segment().abspath << " to " << new_abspath << " because the destination already exists";
        throw std::runtime_error(ss.str());
    }

    auto target_metadata = sys::with_suffix(new_abspath, ".metadata");
    auto target_summary = sys::with_suffix(new_abspath, ".summary");

    // Remove stale metadata and summaries that may have been left around
    std::filesystem::remove(target_metadata);
    std::filesystem::remove(target_summary);

    std::filesystem::create_directories(new_abspath.parent_path());

    this->move_data(new_root, new_relpath, new_abspath);

    // Move metadata to destination
    sys::rename_ifexists(sys::with_suffix(this->segment().abspath, ".metadata"), target_metadata);
    sys::rename_ifexists(sys::with_suffix(this->segment().abspath, ".summary"), target_summary);

    // TODO: get a segment as argument to the whole function?
    auto segment = std::make_shared<Segment>(this->segment().format, new_root, new_relpath);
    auto data = segment->detect_data();
    // TODO: what is really needed as a return value?
    return data->checker(false);
}

}

#endif
