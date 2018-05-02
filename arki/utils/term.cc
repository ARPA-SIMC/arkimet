#include "term.h"
#include <unistd.h>
#include <cerrno>
#include <system_error>

namespace arki {
namespace utils {
namespace term {


#define ANSI_COLOR_RED     "\x1b[31m"
#define ANSI_COLOR_GREEN   "\x1b[32m"
#define ANSI_COLOR_YELLOW  "\x1b[33m"
#define ANSI_COLOR_BLUE    "\x1b[34m"
#define ANSI_COLOR_MAGENTA "\x1b[35m"
#define ANSI_COLOR_CYAN    "\x1b[36m"
#define ANSI_COLOR_RESET   "\x1b[0m"

const unsigned Terminal::black   = 1;
const unsigned Terminal::red     = 2;
const unsigned Terminal::green   = 3;
const unsigned Terminal::yellow  = 4;
const unsigned Terminal::blue    = 5;
const unsigned Terminal::magenta = 6;
const unsigned Terminal::cyan    = 7;
const unsigned Terminal::white   = 8;
const unsigned Terminal::bright  = 0x10;
static const unsigned color_mask = 0xf;

Terminal::Restore::Restore(Terminal& term)
    : term(term)
{
}

Terminal::Restore::~Restore()
{
    if (!term.isatty) return;
    fputs("\x1b[0m", term.out);
}


Terminal::Terminal(FILE* out)
    : out(out)
{
    int fd = fileno(out);
    if (fd == -1)
        isatty = false;
    else
    {
        int res = ::isatty(fd);
        if (res == 1)
            isatty = true;
        else if (errno == EINVAL || errno == ENOTTY)
            isatty = false;
        else
            throw std::system_error(errno, std::system_category(), "isatty failed");
    }
}

namespace {

struct SGR
{
    std::string seq;
    bool first = true;

    SGR() : seq("\x1b[") {}

    void append(int code)
    {
        if (first)
            first = false;
        else
            seq += ";";
        seq += std::to_string(code);
    }

    void end()
    {
        seq += "m";
    }

    void set_fg(int col)
    {
        if (col & Terminal::bright)
            append(1);
        if (col & color_mask)
            append(29 + (col & color_mask));
    }

    void set_bg(int col)
    {
        if ((col & Terminal::bright) && (col & color_mask))
        {
            append(99 + (col & color_mask));
        } else if (col & color_mask) {
            append(89 + (col & color_mask));
        }
    }
};

}

Terminal::Restore Terminal::set_color(int fg, int bg)
{
    if (!isatty) return Restore(*this);

    SGR set;
    if (fg) set.set_fg(fg);
    if (bg) set.set_bg(bg);
    set.end();
    fputs(set.seq.c_str(), out);
    return Restore(*this);
}

Terminal::Restore Terminal::set_color_fg(int col)
{
    return set_color(col, 0);
}

Terminal::Restore Terminal::set_color_bg(int col)
{
    return set_color(0, col);
}

std::string Terminal::color(int fg, int bg, const std::string& s)
{
    if (!isatty) return s;

    SGR set;
    if (fg) set.set_fg(fg);
    if (bg) set.set_bg(bg);
    set.end();
    set.seq += s;
    set.seq += "\x1b[0m";
    return set.seq;
}

std::string Terminal::color_fg(int col, const std::string& s)
{
    return color(col, 0, s);
}

std::string Terminal::color_bg(int col, const std::string& s)
{
    return color(0, col, s);
}


}
}
}
