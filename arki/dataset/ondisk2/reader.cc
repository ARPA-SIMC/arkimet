#include "config.h"
#include "arki/dataset/ondisk2/reader.h"
#include "arki/dataset/index/contents.h"
#include "arki/dataset/targetfile.h"
#include "arki/dataset/archive.h"
#include "arki/types/assigneddataset.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/matcher.h"
#include "arki/utils.h"
#include "arki/utils/files.h"
#include "arki/summary.h"
#include "arki/postprocess.h"
#include "arki/nag.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/wibble/exception.h"
#include <fstream>
#include <sstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#ifdef HAVE_LUA
#include "arki/report.h"
#endif

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace ondisk2 {

Reader::Reader(const ConfigFile& cfg)
    : Local(cfg), m_idx(0), m_tf(0)
{
    this->cfg = cfg.values();
    m_tf = TargetFile::create(cfg);
    if (sys::access(str::joinpath(m_path, "index.sqlite"), F_OK))
    {
        m_idx = new index::RContents(cfg);
        m_idx->open();
    }
}

Reader::~Reader()
{
	if (m_idx) delete m_idx;
	if (m_tf) delete m_tf;
}

void Reader::queryLocalData(const dataset::DataQuery& q, metadata_dest_func dest)
{
    if (!m_idx || !m_idx->query(q, dest))
        throw wibble::exception::Consistency("querying " + m_path, "index could not be used");
}

void Reader::query_data(const dataset::DataQuery& q, metadata_dest_func dest)
{
    // Query the archives first
    Local::query_data(q, dest);
    if (!m_idx) return;
    queryLocalData(q, dest);
}

void Reader::querySummary(const Matcher& matcher, Summary& summary)
{
    // Query the archives first
    Local::querySummary(matcher, summary);
    if (!m_idx) return;
    if (!m_idx || !m_idx->querySummary(matcher, summary))
        throw wibble::exception::Consistency("querying " + m_path, "index could not be used");
}

size_t Reader::produce_nth(metadata::Eater& cons, size_t idx)
{
    size_t res = Local::produce_nth(cons, idx);
    if (m_idx)
    {
        //ds::MakeAbsolute mkabs(cons);
        res += m_idx->produce_nth(cons, idx);
    }
    return res;
}

}
}
}
