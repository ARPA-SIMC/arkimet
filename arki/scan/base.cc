#include "base.h"
#include "arki/utils/sys.h"
#include "arki/utils/files.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace scan {

Scanner::~Scanner()
{
}

void Scanner::open(const std::string& filename, const std::string& basedir, const std::string& relname)
{
    close();
    this->filename = filename;
    this->basedir = basedir;
    this->relname = relname;
}

void Scanner::close()
{
    filename.clear();
    basedir.clear();
    relname.clear();
    reader.reset();
}

void Scanner::test_open(const std::string& filename)
{
    string basedir, relname;
    utils::files::resolve_path(filename, basedir, relname);
    open(sys::abspath(filename), basedir, relname);
}

}
}
