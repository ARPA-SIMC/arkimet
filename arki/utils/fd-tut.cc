#include "config.h"
#include <arki/tests/tests.h>
#include <arki/utils/fd.h>
#include <arki/utils/sys.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::utils::fd;
using namespace arki::tests;

struct arki_utils_fd_shar {
    arki_utils_fd_shar()
    {
    }
};
TESTGRP(arki_utils_fd);

// Check TempfileHandleWatch
template<> template<>
void to::test<1>()
{
    using namespace utils;
    const char* tmpfname = "tfhw.tmp";

    int fd = open(tmpfname, O_WRONLY | O_CREAT | O_EXCL, 0666);
    ensure(fd >= 0);

    {
        TempfileHandleWatch tfhw(tmpfname, fd);
        ensure(sys::exists(tmpfname));
    }
    ensure(!sys::exists(tmpfname));
}

}
