
/*
 * utils/pcounter - Persistent counter
 *
 * Copyright (C) 2010 ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Guido Billi <guidobilli@gmail.com>
 */

#include <arki/utils/pcounter.h>

/*=============================================================================*/

#include <stdexcept>
#include <string>
#if defined(WIN32)
	#include <windows.h>
#elif defined(__linux__) || defined(linux)
	#include <sys/stat.h>
	#include <errno.h>
#endif

namespace arki { namespace utils {

/*=============================================================================*/

bool PersistentCounter_fexists__(const std::string& path)
{
	#if defined(WIN32)
	{
		char*	fileName = (char*)path.c_str();
		DWORD	fileAttr = GetFileAttributes(fileName);
		if (fileAttr == INVALID_FILE_ATTRIBUTES)
		{
			DWORD err = GetLastError();
			if ((err == ERROR_FILE_NOT_FOUND) || (err == ERROR_PATH_NOT_FOUND))
				return false;
			throw std::runtime_error("Unable to check file existance for " + path);
		}
		return true;
	}
	#elif defined(__linux__)
	{
		struct stat dirstats;
		if (stat(path.c_str(), &dirstats) == -1)
		{
			if ((errno == ENOENT) || (errno == ENOTDIR))
				return false;
			throw std::runtime_error("Unable to check file existance for " + path);
		}
		return true;

	}
	#else
		#error fexists not defined on this platform!
	#endif
}

/*=============================================================================*/

} }













