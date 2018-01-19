#include "base.h"
#include "arki/reader.h"
#include "arki/core/file.h"
#include "arki/utils/sys.h"
#include "arki/utils/files.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace scan {

Scanner::~Scanner()
{
}

void Scanner::open(const std::string& filename, const std::string& basedir, const std::string& relname, std::shared_ptr<core::Lock> lock)
{
    close();
    this->filename = filename;
    this->basedir = basedir;
    this->relname = relname;
    reader = Reader::create_new(filename, lock);
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
    open(sys::abspath(filename), basedir, relname, make_shared<core::lock::Null>());
}

}
}
