#include "config.h"
#include "arki/dataset/simple/reader.h"
#include "arki/dataset/index/manifest.h"
#include "arki/configfile.h"
#include "arki/summary.h"
#include "arki/types/reftime.h"
#include "arki/matcher.h"
#include "arki/metadata/collection.h"
#include "arki/utils/files.h"
#include "arki/utils/compress.h"
#include "arki/scan/any.h"
#include "arki/postprocess.h"
#include "arki/sort.h"
#include "arki/nag.h"
#include "arki/utils/sys.h"
#include "arki/utils/string.h"
#include <fstream>
#include <ctime>
#include <cstdio>
#ifdef HAVE_LUA
#include "arki/report.h"
#endif

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace simple {

Reader::Reader(const ConfigFile& cfg)
    : SegmentedLocal(cfg), m_mft(0)
{
    // Create the directory if it does not exist
    sys::makedirs(m_path);

    if (index::Manifest::exists(m_path))
    {
        unique_ptr<index::Manifest> mft = index::Manifest::create(m_path);

		m_mft = mft.release();
		m_mft->openRO();
	}
}

Reader::~Reader()
{
	if (m_mft) delete m_mft;
}

bool Reader::is_dataset(const std::string& dir)
{
    return index::Manifest::exists(dir);
}

void Reader::query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    Local::query_data(q, dest);
    if (!m_mft) return;
    m_mft->query_data(q, dest);
}

void Reader::querySummary(const Matcher& matcher, Summary& summary)
{
	Local::querySummary(matcher, summary);
	if (!m_mft) return;
	m_mft->querySummary(matcher, summary);
}

size_t Reader::produce_nth(metadata_dest_func cons, size_t idx)
{
    size_t res = Local::produce_nth(cons, idx);
    if (m_mft)
        res += m_mft->produce_nth(cons, idx);
    return res;
}

void Reader::maintenance(maintenance::MaintFileVisitor& v)
{
    if (!m_mft) return;
    m_mft->check(*m_segment_manager, v);
}

}
}
}
