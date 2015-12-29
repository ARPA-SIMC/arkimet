/*
 * utils/pcounter - Persistent counter
 *
 * Copyright (C) 2010--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
 * Author: Enrico Zini <enrico@enricozini.org>
 */

#ifndef ARKI_UTILS_PERSISTENTCOUNTER_H
#define ARKI_UTILS_PERSISTENTCOUNTER_H

/*=============================================================================*/

#include <stdexcept>
#include <iomanip>
#include <limits>
#include <string>
#include <sstream>
#include <cstdio>

namespace arki {
namespace utils {

/*=============================================================================*/

/* hidden implementation to avoid header inclusion */
bool PersistentCounter_fexists__(const std::string& path);

template <typename TYPE> class PersistentCounter
{
public:
	PersistentCounter()
	:m_path()
	,m_value(0)
	,m_file(NULL)
	{
	}
	PersistentCounter(const std::string& path)
	:m_path()
	,m_value(0)
	,m_file(NULL)
	{
		bind(path);
	}
	~PersistentCounter()
	{
		try
		{
			unbind();
		}
		catch (...)
		{
		}
	}
	/* bind the counter to the given file */
	void bind(const std::string& path)
	{
		try
		{
			if (PersistentCounter_fexists__(path))
			{
				m_file = fopen(path.c_str(), "r+");
				if (!m_file)
					throw std::runtime_error("Unable to bind persistent counter to " + path + ": file open failed");
				load();
			}
			else
			{
				m_file = fopen(path.c_str(), "w+");
				if (!m_file)
					throw std::runtime_error("Unable to bind persistent counter to " + path + ": file creation failed");
			}
		}
		catch (...)
		{
			if (m_file)
				fclose(m_file);
			throw;
		}
	}
	/* unbind and reset the counter from the file */
	void unbind()
	{
		if (m_file) {
			save(m_value);
			if (fclose(m_file) == EOF)
				throw std::runtime_error("Unable to unbind persistent counter from " + m_path + ": file close failed");
			m_file		= NULL;
			m_path		= "";
			m_value		= 0;
		}
	}

	const std::string& getBindPath() const
	{
		return m_path;
	}

	/* get the current value of the bind the counter to the given file */
	inline TYPE get()
	{
		return m_value;
	}

	TYPE inc()
	{
		return inc(1);
	}

	TYPE inc(TYPE offset)
	{
		TYPE val = m_value;	/* we use a temp value first to handle exceptions */

		if ((std::numeric_limits<TYPE>::max() - offset) < val)
			val = offset - (std::numeric_limits<TYPE>::max() - val);
		else
			val += offset;

		save(val);
		m_value = val;

		return m_value;
	}

	void set(TYPE val)
	{
		/* save val and then change current memory value to handle exceptions*/
		save(val);
		m_value = val;
	}

protected:
	std::string	m_path;
	TYPE	    	m_value;
	FILE*		m_file;

	void load()
	{
		char	buff[1024+1]; /* float std::fixed values can be veeey long */
		size_t	count = fread(buff, 1, sizeof(buff)-1, m_file);
		if ((count==0) && !feof(m_file))
			throw std::runtime_error("Unable to read persistent counter value from " + m_path);
		buff[count] = '\0';

		TYPE val; /* we use a temp value first to handle exceptions */
		std::istringstream ss(buff);
		ss >> val;
		if (ss.fail())
			throw std::runtime_error("Unable to parse persistent counter value '" + ss.str() + "'");
		m_value = val;
	}
	void save(TYPE value)
	{
		if (fseeko(m_file, 0, SEEK_SET))
			throw std::runtime_error("Unable to save persistent counter value to "+m_path+": seek failed");

		std::ostringstream ss;
		ss << std::fixed << value << " ";	/* we add a space to handle possible garbage charaters */
		std::string buff = ss.str();

		if (fwrite(buff.c_str(), buff.length(), 1, m_file) != 1)
			throw std::runtime_error("Unable to save persistent counter value to "+m_path+": write failed");
	}

	static bool fexists(const std::string& path);
};

template <class TYPE> std::ostream& operator<<(std::ostream& ss, PersistentCounter<TYPE>& val)
{
	ss << val.get();
	return ss;
}

template <class TYPE> TYPE& operator<<(TYPE& left, PersistentCounter<TYPE>& right)
{
	left = right.get();
	return left;
}

/*=============================================================================*/

}
}

#endif
