#ifndef ARKI_SEGMENT_BASE_TCC
#define ARKI_SEGMENT_BASE_TCC

#include "arki/segment/data/base.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <sstream>

namespace arki::segment::data {

template<typename Data>
std::shared_ptr<data::Checker> BaseChecker<Data>::move(std::shared_ptr<const segment::Session> segment_session, const std::filesystem::path& new_relpath)
{
    using namespace arki::utils;

    auto new_segment = segment_session->segment_from_relpath_and_format(new_relpath, segment().format());

    // Sanity checks: avoid conflicts
    if (std::filesystem::exists(new_segment->abspath()) ||
            std::filesystem::exists(sys::with_suffix(new_segment->abspath(), ".tar")) ||
            std::filesystem::exists(sys::with_suffix(new_segment->abspath(), ".gz")) ||
            std::filesystem::exists(sys::with_suffix(new_segment->abspath(), ".zip")))
    {
        std::stringstream ss;
        ss << "cannot move " << this->segment().abspath() << " to " << new_segment->abspath() << " because the destination already exists";
        throw std::runtime_error(ss.str());
    }

    auto target_metadata = sys::with_suffix(new_segment->abspath(), ".metadata");
    auto target_summary = sys::with_suffix(new_segment->abspath(), ".summary");

    // Remove stale metadata and summaries that may have been left around
    std::filesystem::remove(target_metadata);
    std::filesystem::remove(target_summary);

    std::filesystem::create_directories(new_segment->abspath().parent_path());

    this->move_data(new_segment);

    // Move metadata to destination
    sys::rename_ifexists(sys::with_suffix(this->segment().abspath(), ".metadata"), target_metadata);
    sys::rename_ifexists(sys::with_suffix(this->segment().abspath(), ".summary"), target_summary);

    auto data = new_segment->data();
    // TODO: what is really needed as a return value?
    return data->checker();
}

}

#endif
