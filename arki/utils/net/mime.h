#ifndef ARKI_UTILS_NET_MIME_H
#define ARKI_UTILS_NET_MIME_H

/// MIME utilities

#include <string>
#include <map>
#include <arki/wibble/regexp.h>
#include <iosfwd>

namespace arki {
namespace utils {
namespace net {
namespace mime {

struct Reader
{
    wibble::ERegexp header_splitter;

    Reader();

    /**
     * Read a line from the file descriptor.
     *
     * The line is terminated by <CR><LF>. The line terminator is not
     * included in the resulting string.
     *
     * @returns true if a line was read, false if EOF
     *
     * Note that if EOF is returned, res can still be filled with a partial
     * line. This may happen if the connection ends after some data has
     * been sent but before <CR><LF> is sent.
     */
    bool read_line(int sock, std::string& res);

    /**
     * Read MIME headers
     *
     * @return true if there still data to read and headers are terminated
     * by an empty line, false if headers are terminated by EOF
     */
    bool read_headers(int sock, std::map<std::string, std::string>& headers);

    /**
     * Read until boundary is found, sending data to the given file descriptor
     *
     * @param max Maximum number of bytes to read, or 0 for unilimited until boundary
     *
     * @returns true if another MIME part follows, false if it is the last part
     * (boundary has trailing --)
     */
    bool read_until_boundary(int sock, const std::string& boundary, int out, size_t max=0);

    /**
     * Read until boundary is found, sending data to the given ostream
     *
     * @param max Maximum number of bytes to read, or 0 for unilimited until boundary
     *
     * @returns true if another MIME part follows, false if it is the last part
     * (boundary has trailing --)
     */
    bool read_until_boundary(int sock, const std::string& boundary, std::ostream& out, size_t max=0);

    /**
     * Read until boundary is found, sending data to the given ostream
     *
     * @returns true if another MIME part follows, false if it is the last part
     * (boundary has trailing --)
     */
    bool discard_until_boundary(int sock, const std::string& boundary);

    /**
     * Skip until the end of the boundary line
     *
     * @return true if the boundary does not end with --, else false
     */
    bool readboundarytail(int sock);
};

}
}
}
}
#endif
