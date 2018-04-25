#ifndef ARKI_SEGMENT_BASE_TCC
#define ARKI_SEGMENT_BASE_TCC

#include "arki/segment/base.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <sstream>

namespace arki {
namespace segment {

template<typename Segment>
std::shared_ptr<Checker> BaseChecker<Segment>::move(const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath)
{
    using namespace arki::utils;

    sys::makedirs(str::dirname(new_abspath));

    // Sanity checks: avoid conflicts
    if (sys::exists(new_abspath) || sys::exists(new_abspath + ".tar") || sys::exists(new_abspath + ".gz") || sys::exists(new_abspath + ".zip"))
    {
        std::stringstream ss;
        ss << "cannot archive " << this->segment().abspath << " to " << new_abspath << " because the destination already exists";
        throw std::runtime_error(ss.str());
    }

    // Remove stale metadata and summaries that may have been left around
    sys::unlink_ifexists(new_abspath + ".metadata");
    sys::unlink_ifexists(new_abspath + ".summary");

    this->move_data(new_root, new_relpath, new_abspath);

    // Move metadata to destination
    sys::rename_ifexists(this->segment().abspath + ".metadata", new_abspath + ".metadata");
    sys::rename_ifexists(this->segment().abspath + ".summary", new_abspath + ".summary");

    return Segment::make_checker(this->segment().format, new_root, new_relpath, new_abspath);
}

}
}

#endif

