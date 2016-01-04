#include "outbound.h"
#include "step.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/types/assigneddataset.h"
#include "arki/dataset/segment.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/wibble/exception.h"
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

    md.set(types::AssignedDataset::create(m_name, ""));

    try {
        storeBlob(md, reldest);
        return ACQ_OK;
    } catch (std::exception& e) {
        md.add_note("Failed to store in dataset '"+m_name+"': " + e.what());
        return ACQ_ERROR;
    }

    // This should never be reached, but we throw an exception to avoid a
    // warning from the compiler
    throw wibble::exception::Consistency("this code is here to appease a compiler warning", "this code path should never be reached");
}

void Outbound::remove(Metadata&)
{
    throw wibble::exception::Consistency("removing data from outbound dataset", "dataset does not support removing items");
}

void Outbound::removeAll(std::ostream& log, bool writable)
{
    log << m_name << ": cleaning dataset not implemented" << endl;
}

size_t Outbound::repackFile(const std::string& relpath)
{
    throw wibble::exception::Consistency("repacking file " + relpath, "dataset does not support repacking files");
}

void Outbound::rescanFile(const std::string& relpath)
{
    throw wibble::exception::Consistency("rescanning file " + relpath, "dataset does not support rescanning files");
}

size_t Outbound::removeFile(const std::string& relpath, bool withData)
{
    throw wibble::exception::Consistency("removing file " + relpath, "dataset does not support removing files");
}

size_t Outbound::vacuum()
{
    return 0;
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
