#include "validator.h"
#include "arki/scan.h"
#include "arki/core/file.h"
#include "arki/utils.h" // TODO: deprecate
#include <sstream>

namespace arki {
namespace scan {

void Validator::throw_check_error(utils::sys::NamedFileDescriptor& fd, off_t offset, const std::string& msg) const
{
    std::stringstream ss;
    ss << fd.name() << ":" << offset << ": " << format() << " validation failed: " << msg;
    throw std::runtime_error(ss.str());
}

void Validator::throw_check_error(const std::string& msg) const
{
    std::stringstream ss;
    ss << format() << " validation failed: " << msg;
    throw std::runtime_error(ss.str());
}

const Validator& Validator::by_filename(const std::string& filename)
{
    // TODO: deprecate
    return by_format(Scanner::format_from_filename(filename));
}

const Validator& Validator::by_format(const std::string& format)
{
    return scan::Scanner::get_validator(format);
}

}
}
