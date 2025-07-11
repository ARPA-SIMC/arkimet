#include "string.h"
#include <cstdint>
#include <vector>

using namespace std;

namespace arki::utils {
namespace str {

namespace {

/**
 * Return the substring of 'str' without all leading characters for which
 * 'classifier' returns true.
 */
template <typename FUN>
std::string lstrip(const std::string& str, const FUN& classifier)
{
    if (str.empty())
        return str;

    size_t beg = 0;
    while (beg < str.size() && classifier(str[beg]))
        ++beg;

    return str.substr(beg, str.size() - beg + 1);
}

/**
 * Return the substring of 'str' without all trailing characters for which
 * 'classifier' returns true.
 */
template <typename FUN>
std::string rstrip(const std::string& str, const FUN& classifier)
{
    if (str.empty())
        return str;

    size_t end = str.size();
    while (end > 0 && classifier(str[end - 1]))
        --end;

    if (end == 0)
        return std::string();
    else
        return str.substr(0, end);
}

/**
 * Return the substring of 'str' without all leading and trailing characters
 * for which 'classifier' returns true.
 */
template <typename FUN>
std::string strip(const std::string& str, const FUN& classifier)
{
    if (str.empty())
        return str;

    size_t beg = 0;
    size_t end = str.size();
    while (beg < end && classifier(str[beg]))
        ++beg;
    while (beg < end && classifier(str[end - 1]))
        --end;

    return str.substr(beg, end - beg);
}

} // namespace

std::string lstrip(const std::string& str) { return lstrip(str, ::isspace); }

std::string rstrip(const std::string& str) { return rstrip(str, ::isspace); }

std::string strip(const std::string& str) { return strip(str, ::isspace); }

std::string basename(const std::string& pathname)
{
    size_t pos = pathname.rfind("/");
    if (pos == std::string::npos)
        return pathname;
    else
        return pathname.substr(pos + 1);
}

std::string dirname(const std::string& pathname)
{
    if (pathname.empty())
        return ".";

    // Skip trailing separators
    size_t end = pathname.size();
    while (end > 0 && pathname[end - 1] == '/')
        --end;

    // If the result is empty again, then the string was only / characters
    if (!end)
        return "/";

    // Find the previous separator
    end = pathname.rfind("/", end - 1);

    if (end == std::string::npos)
        // No previous separator found, everything should be chopped
        return "."s;
    else
    {
        while (end > 0 && pathname[end - 1] == '/')
            --end;
        if (!end)
            return "/";
        return pathname.substr(0, end);
    }
}

void appendpath(std::string& dest, const char* path2)
{
    if (!*path2)
        return;

    if (dest.empty())
    {
        dest = path2;
        return;
    }

    if (dest[dest.size() - 1] == '/')
        if (path2[0] == '/')
            dest += (path2 + 1);
        else
            dest += path2;
    else if (path2[0] == '/')
        dest += path2;
    else
    {
        dest += '/';
        dest += path2;
    }
}

void appendpath(std::string& dest, const std::string& path2)
{
    if (path2.empty())
        return;

    if (dest.empty())
    {
        dest = path2;
        return;
    }

    if (dest[dest.size() - 1] == '/')
        if (path2[0] == '/')
            dest += path2.substr(1);
        else
            dest += path2;
    else if (path2[0] == '/')
        dest += path2;
    else
    {
        dest += '/';
        dest += path2;
    }
}

std::string normpath(const std::string& pathname)
{
    vector<string> st;
    if (pathname[0] == '/')
        st.push_back("/");

    Split split(pathname, "/");
    for (const auto& i : split)
    {
        if (i == "." || i.empty())
            continue;
        if (i == "..")
            if (st.back() == "..")
                st.emplace_back(i);
            else if (st.back() == "/")
                continue;
            else
                st.pop_back();
        else
            st.emplace_back(i);
    }

    if (st.empty())
        return ".";

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    string res;
    for (const auto& i : st)
        appendpath(res, i);
    return res;
#pragma GCC diagnostic pop
}

Split::const_iterator::const_iterator(const Split& split_)
    : split(&split_), cur()
{
    if (split_.str.empty())
        this->split = nullptr;
    else
    {
        // Ignore leading separators if skip_end is true
        if (split_.skip_empty)
            skip_separators();
        ++*this;
    }
}

Split::const_iterator::~const_iterator() {}

std::string Split::const_iterator::remainder() const
{
    if (end == std::string::npos)
        return std::string();
    else
        return split->str.substr(end);
}

void Split::const_iterator::skip_separators()
{
    const std::string& str = split->str;
    const std::string& sep = split->sep;

    while (end + sep.size() <= str.size())
    {
        unsigned i = 0;
        for (; i < sep.size(); ++i)
            if (str[end + i] != sep[i])
                break;
        if (i < sep.size())
            break;
        else
            end += sep.size();
    }
}

Split::const_iterator& Split::const_iterator::operator++()
{
    if (!split)
        return *this;

    const std::string& str = split->str;
    const std::string& sep = split->sep;
    bool skip_empty        = split->skip_empty;

    /// Convert into an end iterator
    if (end == std::string::npos)
    {
        split = nullptr;
        return *this;
    }

    /// The string ended with an iterator, and we do not skip empty tokens:
    /// return it
    if (end == str.size())
    {
        cur = string();
        end = std::string::npos;
        return *this;
    }

    /// Position of the first character past the token that starts at 'end'
    size_t tok_end;
    if (sep.empty())
        /// If separator is empty, advance one character at a time
        tok_end = end + 1;
    else
    {
        /// The token ends at the next separator
        tok_end = str.find(sep, end);
    }

    /// No more separators found, return from end to the end of the string
    if (tok_end == std::string::npos)
    {
        cur = str.substr(end);
        end = std::string::npos;
        return *this;
    }

    /// We have the boundaries of the current token
    cur = str.substr(end, tok_end - end);

    /// Skip the separator
    end = tok_end + sep.size();

    /// Skip all the following separators if skip_empty is true
    if (skip_empty)
    {
        skip_separators();
        if (end == str.size())
        {
            end = std::string::npos;
            return *this;
        }
    }

    return *this;
}

const std::string& Split::const_iterator::operator*() const { return cur; }
const std::string* Split::const_iterator::operator->() const { return &cur; }

bool Split::const_iterator::operator==(const const_iterator& ti) const
{
    if (!split && !ti.split)
        return true;
    if (split != ti.split)
        return false;
    return end == ti.end;
}

bool Split::const_iterator::operator!=(const const_iterator& ti) const
{
    if (!split && !ti.split)
        return false;
    if (split != ti.split)
        return true;
    return end != ti.end;
}

static const char* hex_byte_table[256] = {
    "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "0a", "0b",
    "0c", "0d", "0e", "0f", "10", "11", "12", "13", "14", "15", "16", "17",
    "18", "19", "1a", "1b", "1c", "1d", "1e", "1f", "20", "21", "22", "23",
    "24", "25", "26", "27", "28", "29", "2a", "2b", "2c", "2d", "2e", "2f",
    "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "3a", "3b",
    "3c", "3d", "3e", "3f", "40", "41", "42", "43", "44", "45", "46", "47",
    "48", "49", "4a", "4b", "4c", "4d", "4e", "4f", "50", "51", "52", "53",
    "54", "55", "56", "57", "58", "59", "5a", "5b", "5c", "5d", "5e", "5f",
    "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "6a", "6b",
    "6c", "6d", "6e", "6f", "70", "71", "72", "73", "74", "75", "76", "77",
    "78", "79", "7a", "7b", "7c", "7d", "7e", "7f", "80", "81", "82", "83",
    "84", "85", "86", "87", "88", "89", "8a", "8b", "8c", "8d", "8e", "8f",
    "90", "91", "92", "93", "94", "95", "96", "97", "98", "99", "9a", "9b",
    "9c", "9d", "9e", "9f", "a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7",
    "a8", "a9", "aa", "ab", "ac", "ad", "ae", "af", "b0", "b1", "b2", "b3",
    "b4", "b5", "b6", "b7", "b8", "b9", "ba", "bb", "bc", "bd", "be", "bf",
    "c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "ca", "cb",
    "cc", "cd", "ce", "cf", "d0", "d1", "d2", "d3", "d4", "d5", "d6", "d7",
    "d8", "d9", "da", "db", "dc", "dd", "de", "df", "e0", "e1", "e2", "e3",
    "e4", "e5", "e6", "e7", "e8", "e9", "ea", "eb", "ec", "ed", "ee", "ef",
    "f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "fa", "fb",
    "fc", "fd", "fe", "ff"};

std::string encode_cstring(const std::string& str)
{
    string res;
    for (string::const_iterator i = str.begin(); i != str.end(); ++i)
        if (*i == '\n')
            res += "\\n";
        else if (*i == '\t')
            res += "\\t";
        else if (*i == 0 || iscntrl(*i))
        {
            res += "\\x";
            res += hex_byte_table[static_cast<unsigned char>(*i)];
        }
        else if (*i == '"' || *i == '\\')
        {
            res += "\\";
            res += *i;
        }
        else
            res += *i;
    return res;
}

std::string decode_cstring(const std::string& str, size_t& lenParsed)
{
    string res;
    string::const_iterator i = str.begin();
    for (; i != str.end() && *i != '"'; ++i)
        if (*i == '\\' && (i + 1) != str.end())
        {
            switch (*(i + 1))
            {
                case 'n': res += '\n'; break;
                case 't': res += '\t'; break;
                case 'x': {
                    size_t j;
                    char buf[5] = "0x\0\0";
                    // Read up to 2 extra hex digits
                    for (j = 0; j < 2 && i + 2 + j != str.end() &&
                                isxdigit(*(i + 2 + j));
                         ++j)
                        buf[2 + j] = *(i + 2 + j);
                    i += j;
                    res += static_cast<char>(atoi(buf));
                    break;
                }
                default: res += *(i + 1); break;
            }
            ++i;
        }
        else
            res += *i;
    if (i != str.end() && *i == '"')
        ++i;
    lenParsed = i - str.begin();
    return res;
}

std::string encode_url(const std::string& str)
{
    string res;
    for (string::const_iterator i = str.begin(); i != str.end(); ++i)
    {
        if ((*i >= '0' && *i <= '9') || (*i >= 'A' && *i <= 'Z') ||
            (*i >= 'a' && *i <= 'z') || *i == '-' || *i == '_' || *i == '!' ||
            *i == '*' || *i == '\'' || *i == '(' || *i == ')')
            res += *i;
        else
        {
            char buf[4];
            snprintf(buf, 4, "%%%02x",
                     static_cast<unsigned>(static_cast<unsigned char>(*i)));
            res += buf;
        }
    }
    return res;
}

std::string decode_url(const std::string& str)
{
    string res;
    for (size_t i = 0; i < str.size(); ++i)
    {
        if (str[i] == '%')
        {
            // If there's a partial %something at the end, ignore it
            if (i >= str.size() - 2)
                return res;
            res += static_cast<char>(
                strtoul(str.substr(i + 1, 2).c_str(), nullptr, 16));
            i += 2;
        }
        else
            res += str[i];
    }
    return res;
}

static const char* base64 =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

template <typename T> static char invbase64(const T& idx)
{
    static const char data[] = {
        62, 0,  0,  0,  63, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 0,
        0,  0,  0,  0,  0,  0,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9,
        10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
        0,  0,  0,  0,  0,  0,  26, 27, 28, 29, 30, 31, 32, 33, 34, 35,
        36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51};
    if (idx < 43)
        return 0;
    if (static_cast<unsigned>(idx) > 43 + (sizeof(data) / sizeof(data[0])))
        return 0;
    return data[idx - 43];
}

std::string encode_base64(const std::string& str)
{
    return encode_base64(str.data(), str.size());
}

std::string encode_base64(const void* data, size_t size)
{
    std::string res;
    const uint8_t* str = static_cast<const uint8_t*>(data);

    for (size_t i = 0; i < size; i += 3)
    {
        // Pack every triplet into 24 bits
        unsigned int enc;
        if (i + 3 < size)
            enc = (str[i] << 16) | (str[i + 1] << 8) | str[i + 2];
        else
        {
            enc = (str[i] << 16);
            if (i + 1 < size)
                enc |= str[i + 1] << 8;
            if (i + 2 < size)
                enc |= str[i + 2];
        }

        // Divide in 4 6-bit values and use them as indexes in the base64 char
        // array
        for (int j = 18; j >= 0; j -= 6)
            res += base64[(enc >> j) & 63];
    }

    // Replace padding characters with '='
    if (size % 3)
        for (size_t i = 0; i < 3 - (size % 3); ++i)
            res[res.size() - i - 1] = '=';

    return res;
}

std::string decode_base64(const std::string& str)
{
    std::string res;

    for (size_t i = 0; i < str.size(); i += 4)
    {
        // Pack every quadruplet into 24 bits
        unsigned int enc;
        if (i + 4 < str.size())
        {
            enc = (invbase64(str[i]) << 18) + (invbase64(str[i + 1]) << 12) +
                  (invbase64(str[i + 2]) << 6) + (invbase64(str[i + 3]));
        }
        else
        {
            enc = (invbase64(str[i]) << 18);
            if (i + 1 < str.size())
                enc += (invbase64(str[i + 1]) << 12);
            if (i + 2 < str.size())
                enc += (invbase64(str[i + 2]) << 6);
            if (i + 3 < str.size())
                enc += (invbase64(str[i + 3]));
        }

        // Divide in 3 8-bit values and append them to the result
        res += static_cast<unsigned char>((enc >> 16) & 0xff);
        res += static_cast<unsigned char>((enc >> 8) & 0xff);
        res += static_cast<unsigned char>(enc & 0xff);
    }

    // Remove trailing padding
    if (str.size() > 0)
        for (size_t i = str.size() - 1; str[i] == '='; --i)
        {
            if (res.size() > 0)
                res.resize(res.size() - 1);
            if (i == 0 || res.size() == 0)
                break;
        }

    return res;
}

} // namespace str
} // namespace arki::utils