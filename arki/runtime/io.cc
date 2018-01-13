#include "config.h"
#include <arki/runtime/io.h>
#include <arki/runtime/config.h>
#include <arki/metadata/consumer.h>
#include <arki/utils/string.h>
#include <iostream>
#include <cstdlib>
#include <cstring>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace std;
using namespace arki::core;
using namespace arki::utils;

namespace arki {
namespace runtime {

InputFile::InputFile(const std::string& pathname)
    : sys::File(pathname, O_RDONLY)
{
}

std::unique_ptr<utils::sys::NamedFileDescriptor> make_input(utils::commandline::Parser& opts)
{
    if (!opts.hasNext())
        return std::unique_ptr<utils::sys::NamedFileDescriptor>(new Stdin);

    string pathname = opts.next();
    if (pathname == "-")
        return std::unique_ptr<utils::sys::NamedFileDescriptor>(new Stdin);

    return std::unique_ptr<utils::sys::NamedFileDescriptor>(new InputFile(pathname));
}

File::File(const std::string pathname, bool append)
    : sys::File(pathname, O_WRONLY | O_CREAT | (append ? O_APPEND : O_TRUNC), 0666)
{
}

std::unique_ptr<sys::NamedFileDescriptor> make_output(utils::commandline::Parser& opts)
{
    if (opts.hasNext())
    {
        string pathname = opts.next();
        if (pathname != "-")
            return unique_ptr<sys::NamedFileDescriptor>(new File(pathname));
    }
    return unique_ptr<sys::NamedFileDescriptor>(new Stdout);
}

std::unique_ptr<sys::NamedFileDescriptor> make_output(utils::commandline::StringOption& opt)
{
    if (opt.isSet())
    {
        string pathname = opt.value();
        if (pathname != "-")
            return unique_ptr<sys::NamedFileDescriptor>(new File(pathname));
    }
    return unique_ptr<sys::NamedFileDescriptor>(new Stdout);
}

Tempfile::Tempfile() : sys::File(sys::File::mkstemp("arkimet")) {}

Tempfile::~Tempfile()
{
    if (m_unlink_on_exit)
        ::unlink(name().c_str());
}

void Tempfile::unlink_on_exit(bool val)
{
    m_unlink_on_exit = val;
}

void Tempfile::unlink()
{
    sys::unlink(name());
}

}
}
