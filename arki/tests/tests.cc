#include "config.h"
#include <arki/tests/tests.h>
#include <arki/utils/sys.h>
#include <sys/fcntl.h>
#include <sstream>

using namespace std;
using namespace arki;
using namespace arki::utils;

namespace arki {
namespace tests {

std::string tempfile_to_string(std::function<void(arki::utils::sys::NamedFileDescriptor& out)> body)
{
    sys::File wr("tempfile", O_WRONLY | O_CREAT | O_TRUNC, 0666);
    body(wr);
    wr.close();
    string res = sys::read_file("tempfile");
    sys::unlink("tempfile");
    return res;
}

}
}
