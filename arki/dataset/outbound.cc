#include "outbound.h"
#include "step.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/dataset/segment.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <sys/stat.h>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {

Outbound::Outbound(const ConfigFile& cfg)
    : SegmentedWriter(cfg)
{
}

Outbound::~Outbound()
{
}

std::string Outbound::type() const { return "outbound"; }

void Outbound::storeBlob(Metadata& md, const std::string& reldest)
{
    // Write using segment::Writer
    Segment* w = file(md, md.source().format);
    w->append(md);
}

Writer::AcquireResult Outbound::acquire(Metadata& md, ReplaceStrategy replace)
{
    string reldest = (*m_step)(md);
    string dest = m_path + "/" + reldest;

    sys::makedirs(str::dirname(dest));

    try {
        storeBlob(md, reldest);
        return ACQ_OK;
    } catch (std::exception& e) {
        md.add_note("Failed to store in dataset '"+m_name+"': " + e.what());
        return ACQ_ERROR;
    }

    // This should never be reached, but we throw an exception to avoid a
    // warning from the compiler
    throw std::runtime_error("this code path should never be reached (it is here to appease a compiler warning)");
}

void Outbound::remove(Metadata&)
{
    throw std::runtime_error("cannot remove data from outbound dataset: dataset does not support removing items");
}

void Outbound::removeAll(std::ostream& log, bool writable)
{
    log << m_name << ": cleaning dataset not implemented" << endl;
}

Writer::AcquireResult Outbound::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
    unique_ptr<Step> tf(Step::create(cfg));
    string dest = cfg.value("path") + "/" + (*tf)(md) + "." + md.source().format;
    out << "Assigning to dataset " << cfg.value("name") << " in file " << dest << endl;
    return ACQ_OK;
}

}
}
