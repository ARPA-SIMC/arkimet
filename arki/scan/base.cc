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

void Scanner::open(const std::string& filename, const std::string& basedir, const std::string& relpath, std::shared_ptr<core::Lock> lock)
{
    close();
    this->filename = filename;
    this->basedir = basedir;
    this->relpath = relpath;
    reader = Reader::create_new(filename, lock);
}

void Scanner::close()
{
    filename.clear();
    basedir.clear();
    relpath.clear();
    reader.reset();
}

void Scanner::test_open(const std::string& filename)
{
    string basedir, relpath;
    utils::files::resolve_path(filename, basedir, relpath);
    open(sys::abspath(filename), basedir, relpath, make_shared<core::lock::Null>());
}

}
}
