#include "file.h"
#include <algorithm>
#include <deque>

namespace arki {

Stdin::Stdin() : NamedFileDescriptor(0, "(stdin)") {}
Stdout::Stdout() : NamedFileDescriptor(1, "(stdout)") {}
Stderr::Stderr() : NamedFileDescriptor(2, "(stderr)") {}

namespace {

struct FDLineReader : public LineReader
{
    NamedFileDescriptor& fd;
    std::deque<char> linebuf;
    bool fd_eof = false;

    FDLineReader(NamedFileDescriptor& fd) : fd(fd) {}

    bool eof() const override { return fd_eof && linebuf.empty(); }

    bool getline(std::string& line) override
    {
        line.clear();
        while (true)
        {
            std::deque<char>::iterator i = std::find(linebuf.begin(), linebuf.end(), '\n');
            if (i != linebuf.end())
            {
                line.append(linebuf.begin(), i);
                linebuf.erase(linebuf.begin(), i + 1);
                return true;
            }

            if (fd_eof)
            {
                if (i == linebuf.end())
                    return false;

                line.append(linebuf.begin(), linebuf.end());
                linebuf.clear();
                return true;
            }

            // Read more and retry
            char buf[4096];
            size_t count = fd.read(buf, 4096);
            if (count == 0)
                fd_eof = true;
            else
                linebuf.insert(linebuf.end(), buf, buf + count);
        }
    }
};

struct StringLineReader : public LineReader
{
    const char* cur;
    const char* end;

    StringLineReader(const char* buf, size_t size)
        : cur(buf), end(buf + size) {}

    bool eof() const override { return cur == end; }

    bool getline(std::string& line) override
    {
        const char* pos = std::find(cur, end, '\n');
        if (pos == end)
        {
            if (cur == end)
            {
                line.clear();
                return false;
            }

            line.assign(cur, end);
            cur = end;
            return true;
        }

        line.assign(cur, pos);
        cur = pos + 1;
        return true;
    }
};

}


std::unique_ptr<LineReader> LineReader::from_fd(NamedFileDescriptor& fd)
{
    return std::unique_ptr<LineReader>(new FDLineReader(fd));
}

std::unique_ptr<LineReader> LineReader::from_chars(const char* buf, size_t size)
{
    return std::unique_ptr<LineReader>(new StringLineReader(buf, size));
}

}
