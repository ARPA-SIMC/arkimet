#include "mime.h"
#include "arki/exceptions.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include <unistd.h>

using namespace std;

namespace arki {
namespace utils {
namespace net {
namespace mime {

Reader::Reader() 
      : header_splitter("^([^:[:blank:]]+)[[:blank:]]*:[[:blank:]]*(.+)", 3)
{
}

bool Reader::read_line(int sock, string& res)
{
    bool has_cr = false;
    res.clear();
    while (true)
    {
        char c;
        ssize_t count = read(sock, &c, 1);
        if (count == 0) return false;
        if (count < 0)
            throw_system_error("reading from socket");
        switch (c)
        {
            case '\r':
                if (has_cr)
                    res.append(1, '\r');
                has_cr = true;
                break;
            case '\n':
                if (has_cr)
                    return true;
                else
                    res.append(1, '\n');
                break;
            default:
                res.append(1, c);
                break;
        }
    }
}
    
bool Reader::read_headers(int sock, std::map<std::string, std::string>& headers)
{
    string line;
    map<string, string>::iterator last_inserted = headers.end();
    while (read_line(sock, line))
    {
        // Headers are terminated by an empty line
        if (line.empty())
            return true;

        if (isblank(line[0]))
        {
            // Append continuation of previous header body
            if (last_inserted != headers.end())
            {
                last_inserted->second.append(" ");
                last_inserted->second.append(str::strip(line));
            }
        } else {
            if (header_splitter.match(line))
            {
                // rfc2616, 4.2 Message Headers:
                // Multiple message-header fields with
                // the same field-name MAY be present
                // in a message if and only if the
                // entire field-value for that header
                // field is defined as a
                // comma-separated list [i.e.,
                // #(values)].  It MUST be possible to
                // combine the multiple header fields
                // into one "field-name: field-value"
                // pair, without changing the semantics
                // of the message, by appending each
                // subsequent field-value to the first,
                // each separated by a comma.
                string key = str::lower(header_splitter[1]);
                string val = str::strip(header_splitter[2]);
                map<string, string>::iterator old = headers.find(key);
                if (old == headers.end())
                {
                    // Insert new
                    pair< map<string, string>::iterator, bool > res =
                        headers.insert(make_pair(key, val));
                    last_inserted = res.first;
                } else {
                    // Append comma-separated
                    old->second.append(",");
                    old->second.append(val);
                    last_inserted = old;
                }
            } else
                last_inserted = headers.end();
        }
    }
    return false;
}

bool Reader::readboundarytail(int sock)
{
    // [\-\s]*\r\n
    bool has_cr = false;
    bool has_dash = false;
    while (true)
    {
        char c;
        ssize_t count = read(sock, &c, 1);
        if (count == 0) throw std::runtime_error("reading from socket: data ends before MIME boundary");
        if (count < 0) throw_system_error("reading from socket");
        switch (c)
        {
            case '\r':
                has_cr = true;
                break;
            case '\n':
                if (has_cr) return !has_dash;
                break;
            case '\t':
            case ' ':
                has_cr = false;
                break;
            case '-':
                has_dash = true;
                has_cr = false;
                break;
            default:
                throw std::runtime_error("reading from socket: line ends with non-whitespace");
                break;
        }
    }
}

bool Reader::read_until_boundary(int sock, const std::string& boundary, int outfd, size_t max)
{
    sys::FileDescriptor out(outfd);
    size_t read_so_far = 0;
    unsigned got = 0;

    while (got < boundary.size())
    {
        char c;
        ssize_t count = read(sock, &c, 1);
        if (count == 0) throw std::runtime_error("reading from socket: data ends before MIME boundary");
        if (count < 0) throw_system_error("reading from socket");

        if (c == boundary[got])
            ++got;
        else
        {
            if (max == 0 || read_so_far < max)
            {
                if (got > 0)
                {
                    out.write(boundary.data(), got);
                    read_so_far += got;
                }
                out.write(&c, 1);
                ++read_so_far;
            }
            got = 0;
        }
    }

    return readboundarytail(sock);
}

bool Reader::read_until_boundary(int sock, const std::string& boundary, std::ostream& out, size_t max)
{
    size_t read_so_far = 0;
    unsigned got = 0;

    while (got < boundary.size())
    {
        char c;
        ssize_t count = read(sock, &c, 1);
        if (count == 0) throw std::runtime_error("reading from socket: data ends before MIME boundary");
        if (count < 0) throw_system_error("reading from socket");

        if (c == boundary[got])
            ++got;
        else
        {
            if (max == 0 || read_so_far < max)
            {
                if (got > 0)
                {
                    out.write(boundary.data(), got);
                    read_so_far += got;
                }
                out.put(c);
                ++read_so_far;
            }
            got = 0;
        }
    }

    return readboundarytail(sock);
}

bool Reader::discard_until_boundary(int sock, const std::string& boundary)
{
    unsigned got = 0;

    while (got < boundary.size())
    {
        char c;
        ssize_t count = read(sock, &c, 1);
        if (count == 0) throw std::runtime_error("reading from socket: data ends before MIME boundary");
        if (count < 0) throw_system_error("reading from socket");
        if (c == boundary[got])
            ++got;
        else
            got = 0;
    }

    return readboundarytail(sock);
}

}
}
}
}
