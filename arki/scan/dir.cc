#include "config.h"
#include <arki/scan/dir.h>
#include <arki/scan/any.h>
#include <arki/utils/sys.h>
#include <arki/utils/string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>

using namespace std;
using namespace arki::utils;

namespace arki {
namespace scan {

static void scan(set<string>& res, const std::string& top, const std::string& root, bool files_in_root, int level = 0)
{
    sys::Path dir(root);
    for (sys::Path::iterator i = dir.begin(); i != dir.end(); ++i)
    {
        // Skip '.', '..' and hidden files
        if (i->d_name[0] == '.')
            continue;
        // Skip compressed data index files
        if (str::endswith(i->d_name, ".gz.idx"))
            continue;

        // stat(2) the file
        string pathname = str::joinpath(root, i->d_name);

        bool isdir = i.isdir();
        bool isdirsegment = isdir && sys::exists(str::joinpath(pathname, ".sequence"));

        if (isdir && !isdirsegment)
        {
            // It is a directory, and not a directory segment: recurse into it
            scan(res, top, pathname, files_in_root, level + 1);
        } else if ((files_in_root || level > 0) && (isdirsegment || i.isreg())) {
            if (str::endswith(pathname, ".gz"))
                pathname = pathname.substr(0, pathname.size() - 3);
            if (scan::canScan(pathname))
                // Skip files in the root dir
                // We point to a good file, keep it
                res.insert(pathname.substr(top.size() + 1));
        }
    }
}

std::set<std::string> dir(const std::string& root, bool files_in_root)
{
    string m_root = root;

    // Trim trailing '/'
    while (m_root.size() > 1 and m_root[m_root.size()-1] == '/')
        m_root.resize(m_root.size() - 1);

    set<string> res;
    scan(res, m_root, m_root, files_in_root);
    return res;
}

}
}
