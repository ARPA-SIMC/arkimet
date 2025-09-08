#include "file.h"
#include <cstdio>
#include <cstring>
#include <limits>
#include <sstream>

namespace arki::core {

Stdin::Stdin() : NamedFileDescriptor(0, "(stdin)") {}
Stdout::Stdout() : NamedFileDescriptor(1, "(stdout)") {}
Stderr::Stderr() : NamedFileDescriptor(2, "(stderr)") {}

AbstractInputFile::~AbstractInputFile() {}

/*
 * MemoryFile
 */

MemoryFile::MemoryFile(const void* buffer, size_t size,
                       const std::filesystem::path& path)
    : m_path(path), buffer(buffer), buffer_size(size)
{
}

size_t MemoryFile::read(void* dest, size_t size)
{
    size_t read_size = std::min(size, buffer_size - pos);
    memcpy(dest, static_cast<const uint8_t*>(buffer) + pos, read_size);
    pos += read_size;
    return read_size;
}

/*
 * BufferedReader
 */

BufferedReader::~BufferedReader() {}

bool BufferedReader::refill()
{
    pos = 0;
    end = read();
    if (end == 0)
        return false;
    return true;
}

int BufferedReader::peek()
{
    if (pos >= end)
        if (!refill())
            return EOF;
    return buffer[pos];
}

int BufferedReader::get()
{
    if (pos >= end)
        if (!refill())
            return EOF;
    return buffer[pos++];
}

namespace {

template <typename Input> struct FDBufferedReader : public BufferedReader
{
    Input& fd;

    FDBufferedReader(Input& fd) : fd(fd) {}

    unsigned read() override { return fd.read(buffer.data(), buffer.size()); }
};

struct StringBufferedReader : public BufferedReader
{
    const char* cur;
    const char* end;

    StringBufferedReader(const char* buf, size_t size)
        : cur(buf), end(buf + size)
    {
    }

    unsigned read() override
    {
        if (cur == end)
            return 0;
        unsigned size = buffer.size();
        if (end - cur < size)
            size = end - cur;
        memcpy(buffer.data(), cur, size);
        cur += size;
        return size;
    }
};

template <typename Buffer> struct LineReaderImpl : public LineReader, Buffer
{
    using Buffer::Buffer;

    bool getline(std::string& line) override
    {
        line.clear();
        while (true)
        {
            int c = this->get();
            if (c == EOF)
            {
                this->fd_eof = true;
                return false;
            }

            if (c == '\n')
                return true;

            line += (char)c;
        }
    }
};

} // namespace

std::unique_ptr<BufferedReader> BufferedReader::from_fd(NamedFileDescriptor& fd)
{
    return std::unique_ptr<BufferedReader>(
        new FDBufferedReader<NamedFileDescriptor>(fd));
}

std::unique_ptr<BufferedReader>
BufferedReader::from_abstract(AbstractInputFile& fd)
{
    return std::unique_ptr<BufferedReader>(
        new FDBufferedReader<AbstractInputFile>(fd));
}

std::unique_ptr<BufferedReader> BufferedReader::from_chars(const char* buf,
                                                           size_t size)
{
    return std::unique_ptr<BufferedReader>(new StringBufferedReader(buf, size));
}

std::unique_ptr<BufferedReader>
BufferedReader::from_string(const std::string& str)
{
    return std::unique_ptr<BufferedReader>(
        new StringBufferedReader(str.data(), str.size()));
}

std::unique_ptr<LineReader> LineReader::from_fd(NamedFileDescriptor& fd)
{
    return std::unique_ptr<LineReader>(
        new LineReaderImpl<FDBufferedReader<NamedFileDescriptor>>(fd));
}

std::unique_ptr<LineReader> LineReader::from_abstract(AbstractInputFile& fd)
{
    return std::unique_ptr<LineReader>(
        new LineReaderImpl<FDBufferedReader<AbstractInputFile>>(fd));
}

std::unique_ptr<LineReader> LineReader::from_chars(const char* buf, size_t size)
{
    return std::unique_ptr<LineReader>(
        new LineReaderImpl<StringBufferedReader>(buf, size));
}

std::unique_ptr<LineReader> LineReader::from_string(const std::string& str)
{
    return std::unique_ptr<LineReader>(
        new LineReaderImpl<StringBufferedReader>(str.data(), str.size()));
}

} // namespace arki::core
