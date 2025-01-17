#include "tests.h"
#include "data.h"
#include "arki/utils/sys.h"

using namespace arki::utils;

namespace arki::tests {

void fill_metadata_segment(std::shared_ptr<const Segment> segment, arki::metadata::Collection& mds)
{
    segment::data::WriterConfig wconf{true, true};
    auto data_writer = segment->detect_data()->writer(wconf, false);
    for (auto i: mds)
        data_writer->append(*i);
    data_writer->commit();
    mds.writeAtomically(sys::with_suffix(segment->abspath(), ".metadata"));
}

}
