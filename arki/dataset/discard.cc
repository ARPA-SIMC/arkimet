#include "config.h"
#include <arki/dataset/discard.h>
#include <arki/metadata.h>
#include <arki/configfile.h>
#include <iostream>

using namespace std;

namespace arki {
namespace dataset {

Writer::AcquireResult Discard::acquire(Metadata& md, ReplaceStrategy replace)
{
    return ACQ_OK;
}

Writer::AcquireResult Discard::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
    out << "Resetting dataset information to mark that the message has been discarded" << endl;
    return ACQ_OK;
}

}
}
