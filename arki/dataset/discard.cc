#include "config.h"
#include <arki/dataset/discard.h>
#include <arki/metadata.h>
#include <arki/configfile.h>
#include <arki/types/assigneddataset.h>
#include <iostream>

using namespace std;

namespace arki {
namespace dataset {

Discard::Discard(const ConfigFile& cfg)
{
	m_name = cfg.value("name");
}

Discard::~Discard()
{
}

WritableDataset::AcquireResult Discard::acquire(Metadata& md, ReplaceStrategy replace)
{
	md.set(types::AssignedDataset::create(m_name, ""));
	return ACQ_OK;
}

WritableDataset::AcquireResult Discard::testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out)
{
	out << "Resetting dataset information to mark that the message has been discarded" << endl;
	return ACQ_OK;
}

}
}
