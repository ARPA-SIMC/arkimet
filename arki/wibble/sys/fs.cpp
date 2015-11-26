#include <arki/wibble/sys/fs.h>
#include <arki/wibble/sys/process.h>
#include <arki/wibble/string.h>
#include <arki/wibble/exception.h>
#include <fstream>
#include <dirent.h> // opendir, closedir
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <malloc.h> // alloca on win32 seems to live there

namespace wibble {
namespace sys {
namespace fs {

void stat(const std::string& pathname, struct stat& st)
{
    if (::stat(pathname.c_str(), &st) == -1)
        throw wibble::exception::File(pathname, "getting file information");
}

#define common_stat_body(testfunc) \
    struct stat st; \
    if (::stat(pathname.c_str(), &st) == -1) { \
        if (errno == ENOENT) \
            return false; \
        else \
            throw wibble::exception::System("getting file information for " + pathname); \
    } \
    return testfunc(st.st_mode)

bool isdir(const std::string& pathname)
{
    common_stat_body(S_ISDIR);
}

bool isblk(const std::string& pathname)
{
    common_stat_body(S_ISBLK);
}

bool ischr(const std::string& pathname)
{
    common_stat_body(S_ISCHR);
}

bool isfifo(const std::string& pathname)
{
    common_stat_body(S_ISFIFO);
}

bool islnk(const std::string& pathname)
{
    common_stat_body(S_ISLNK);
}

bool isreg(const std::string& pathname)
{
    common_stat_body(S_ISREG);
}

bool issock(const std::string& pathname)
{
    common_stat_body(S_ISSOCK);
}

#undef common_stat_body

bool access(const std::string &s, int m)
{
	return ::access(s.c_str(), m) == 0;
}

bool exists(const std::string& file)
{
    return sys::fs::access(file, F_OK);
}

std::string abspath(const std::string& pathname)
{
	if (pathname[0] == '/')
		return str::normpath(pathname);
	else
		return str::normpath(str::joinpath(process::getcwd(), pathname));
}

std::string readFile( const std::string &file )
{
    std::ifstream in( file.c_str(), std::ios::binary );
    if (!in.is_open())
        throw wibble::exception::System( "reading file " + file );
    std::string ret;
    size_t length;

    in.seekg(0, std::ios::end);
    length = in.tellg();
    in.seekg(0, std::ios::beg);
    char *buffer = static_cast<char *>( alloca( length ) );

    in.read(buffer, length);
    return std::string( buffer, length );
}

std::string readFile(std::istream& input, const std::string& filename)
{
    static const size_t bufsize = 4096;
    char buf[bufsize];
    std::string res;
    while (true)
    {
        input.read(buf, bufsize);
        res.append(buf, input.gcount());
        if (input.eof())
            break;
        if (input.fail())
            throw wibble::exception::File(filename, "reading data");
    }
    return res;
}

void writeFile( const std::string &file, const std::string &data )
{
    std::ofstream out( file.c_str(), std::ios::binary );
    if (!out.is_open())
        throw wibble::exception::System( "writing file " + file );
    out << data;
}

void writeFileAtomically(const std::string &file, const std::string &data)
{
    char* fbuf = (char*)alloca(file.size() + 7);
    memcpy(fbuf, file.data(), file.size());
    memcpy(fbuf + file.size(), "XXXXXX", 7);
    int fd = mkstemp(fbuf);
    if (fd < 0)
        throw wibble::exception::File(fbuf, "cannot create temp file");
    ssize_t res = write(fd, data.data(), data.size());
    if (res != (ssize_t)data.size())
        throw wibble::exception::File(fbuf, str::fmtf("cannot write %d bytes", data.size()));
    if (close(fd) < 0)
        throw wibble::exception::File(fbuf, "cannot close file");
    if (rename(fbuf, file.c_str()) < 0)
        throw wibble::exception::File(fbuf, "cannot rename to " + file);
}

std::string findExecutable(const std::string& name)
{
    // argv[0] has an explicit path: ensure it becomes absolute
    if (name.find('/') != std::string::npos)
        return sys::fs::abspath(name);

    // argv[0] has no explicit path, look for it in $PATH
    const char* path = getenv("PATH");
    if (path == NULL)
        return name;
    str::Split splitter(":", path);
    for (str::Split::const_iterator i = splitter.begin(); i != splitter.end(); ++i)
    {
        std::string candidate = str::joinpath(*i, name);
        if (sys::fs::access(candidate, X_OK))
            return sys::fs::abspath(candidate);
    }

    return name;
}

bool deleteIfExists(const std::string& file)
{
	if (::unlink(file.c_str()) != 0)
		if (errno != ENOENT)
			throw wibble::exception::File(file, "removing file");
		else
			return false;
	else
		return true;
}

void renameIfExists(const std::string& src, const std::string& dst)
{
    int res = ::rename(src.c_str(), dst.c_str());
    if (res < 0 && errno != ENOENT)
        throw wibble::exception::System("moving " + src + " to " + dst);
}

void unlink(const std::string& fname)
{
    if (::unlink(fname.c_str()) < 0)
        throw wibble::exception::File(fname, "cannot delete file");
}

void rmdir(const std::string& dirname)
{
    if (::rmdir(dirname.c_str()) < 0)
        throw wibble::exception::System("cannot delete directory " + dirname);
}

time_t timestamp(const std::string& file)
{
    struct stat st;
    stat(file, st);
    return st.st_mtime;
}

size_t size(const std::string& file)
{
    struct stat st;
    stat(file, st);
    return (size_t)st.st_size;
}

ino_t inode(const std::string& file)
{
    struct stat st;
    stat(file, st);
    return st.st_ino;
}

std::string mkdtemp( std::string tmpl )
{
    char *_tmpl = reinterpret_cast< char * >( alloca( tmpl.size() + 1 ) );
    strcpy( _tmpl, tmpl.c_str() );
    return ::mkdtemp( _tmpl );
}

}
}
}

// vim:set ts=4 sw=4:
