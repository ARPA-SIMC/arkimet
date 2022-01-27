#ifndef ARKI_STREAM_TEXT_H
#define ARKI_STREAM_TEXT_H

#include <arki/stream.h>
#include <string>

namespace arki {
namespace stream {

struct Text
{
    StreamOutput& out;
    SendResult result;

    Text(StreamOutput& out) : out(out) {}

    void print(const std::string& str)
    {
        if (result.flags & SendResult::SEND_PIPE_EOF_DEST)
            return;

        result += out.send_line(str.data(), str.size());
    }

    /**
     * Print a reStructuredText title.
     *
     * Level represents the heading level (1 being the outermost one), and
     * rendering is done according to
     * https://devguide.python.org/documenting/#sections
     */
    void rst_header(const std::string& str, unsigned level=1)
    {
        bool over = false;
        char c = '"';

        switch (level)
        {
            case 1: over = true, c = '#'; break;
            case 2: over = true, c = '*'; break;
            case 3: c = '='; break;
            case 4: c = '-'; break;
            case 5: c = '^'; break;
        }

        std::string bar(str.size(), c);
        if (over) print(bar);
        print(str);
        print(bar);

    }
};

}
}

#endif
