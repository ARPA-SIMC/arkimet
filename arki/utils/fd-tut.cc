/*
 * Copyright (C) 2007--2010  Enrico Zini <enrico@enricozini.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */

#include <arki/tests/test-utils.h>
#include <arki/utils/fd.h>
#include <arki/utils/files.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <sstream>
#include <iostream>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::utils::fd;

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
        ensure(files::exists(tmpfname));
    }
    ensure(!files::exists(tmpfname));
}

}

// vim:set ts=4 sw=4:
