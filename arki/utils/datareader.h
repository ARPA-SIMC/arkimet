#ifndef ARKI_UTILS_DATAREADER_H
#define ARKI_UTILS_DATAREADER_H

/// Generic interface to read data files
#include <arki/libconfig.h>
#include <cstddef>
#include <string>
#include <sys/types.h>

namespace arki {
namespace utils {

namespace datareader {
struct Reader
{
public:
	virtual ~Reader() {}

	virtual void read(off_t ofs, size_t size, void* buf) = 0;
	virtual bool is(const std::string& fname) = 0;
};
}

/**
 * Read data from files, keeping the last file open.
 *
 * This optimizes for sequentially reading data from files.
 */
class DataReader
{
protected:
	datareader::Reader* last;
	
public:
	DataReader();
	~DataReader();

	void read(const std::string& fname, off_t ofs, size_t size, void* buf);
	void flush();
};

}
}
#endif
