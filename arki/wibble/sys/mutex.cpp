/*
 * Encapsulated pthread mutex and condition
 *
 * Copyright (C) 2003--2006  Enrico Zini <enrico@debian.org>
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

#include <arki/wibble/sys/mutex.h>
#include <errno.h>

namespace wibble {
namespace sys {

#ifdef POSIX
bool Condition::wait(MutexLock& l, const struct timespec& abstime)
{
	if (int res = pthread_cond_timedwait(&cond, &l.mutex.mutex, &abstime)) {
		if (res == ETIMEDOUT)
			return false;
		else
			throw wibble::exception::System(res, "waiting on a pthread condition");
	}
	return true;
}
#endif

}
}

// vim:set ts=4 sw=4:
