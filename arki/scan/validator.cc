#include "validator.h"
#include "arki/exceptions.h"
#include "arki/scan.h"
#include "arki/metadata/data.h"
#include "arki/utils/sys.h"
#include <sstream>

namespace arki {
namespace scan {

void Validator::throw_check_error(utils::sys::NamedFileDescriptor& fd, off_t offset, const std::string& msg) const
{
    throw_runtime_error(fd.path(), ":", offset, ": ", format(), " validation failed: ", msg);
}

void Validator::throw_check_error(const std::string& msg) const
{
    throw_runtime_error(format(), " validation failed: ", msg);
}

const Validator& Validator::by_filename(const std::filesystem::path& filename)
{
    return by_format(Scanner::format_from_filename(filename));
}

const Validator& Validator::by_format(const std::string& format)
{
    return scan::Scanner::get_validator(format);
}

void Validator::validate_data(const metadata::Data& data) const
{
    auto buf = data.read();
    validate_buf(buf.data(), buf.size());
}

}
}
