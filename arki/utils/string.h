#ifndef ARKI-UTILS_STRING_H
#define ARKI-UTILS_STRING_H

/**
 * @author Enrico Zini <enrico@enricozini.org>
 * @brief String functions
 *
 * Copyright (C) 2007--2015  Enrico Zini <enrico@debian.org>
 */

#include <string>
#include <functional>
#include <sstream>
#include <cctype>

namespace arki {
namespace utils {
namespace str {

/// Check if a string starts with the given substring
inline bool startswith(const std::string& str, const std::string& part)
{
    if (str.size() < part.size())
        return false;
    return str.substr(0, part.size()) == part;
}

/// Check if a string ends with the given substring
inline bool endswith(const std::string& str, const std::string& part)
{
    if (str.size() < part.size())
        return false;
    return str.substr(str.size() - part.size()) == part;
}

/**
 * Stringify and join a sequence of objects
 */
template<typename ITER>
std::string join(const std::string& sep, const ITER& begin, const ITER& end)
{
    std::stringstream res;
    bool first = true;
    for (ITER i = begin; i != end; ++i)
    {
        if (first)
            first = false;
        else
            res << sep;
        res << *i;
    }
    return res.str();
}

/**
 * Stringify and join an iterable container
 */
template<typename ITEMS>
std::string join(const std::string& sep, const ITEMS& items)
{
    std::stringstream res;
    bool first = true;
    for (const auto& i: items)
    {
        if (first)
            first = false;
        else
            res << sep;
        res << i;
    }
    return res.str();
}

/**
 * Return the substring of 'str' without all leading characters for which
 * 'classifier' returns true.
 */
template<typename FUN>
inline std::string lstrip(const std::string& str, const FUN& classifier)
{
    if (str.empty())
        return str;

    size_t beg = 0;
    while (beg < str.size() && classifier(str[beg]))
        ++beg;

    return str.substr(beg, str.size() - beg + 1);
}

/**
 * Return the substring of 'str' without all leading spaces.
 */
inline std::string lstrip(const std::string& str)
{
    return lstrip(str, ::isspace);
}

/**
 * Return the substring of 'str' without all trailing characters for which
 * 'classifier' returns true.
 */
template<typename FUN>
inline std::string rstrip(const std::string& str, const FUN& classifier)
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
 * Return the substring of 'str' without all trailing spaces.
 */
inline std::string rstrip(const std::string& str)
{
    return rstrip(str, ::isspace);
}

/**
 * Return the substring of 'str' without all leading and trailing characters
 * for which 'classifier' returns true.
 */
template<typename FUN>
inline std::string strip(const std::string& str, const FUN& classifier)
{
    if (str.empty())
        return str;

    size_t beg = 0;
    size_t end = str.size() - 1;
    while (beg < end && classifier(str[beg]))
        ++beg;
    while (end >= beg && classifier(str[end]))
        --end;

    return str.substr(beg, end-beg+1);
}

/**
 * Return the substring of 'str' without all leading and trailing spaces.
 */
inline std::string strip(const std::string& str)
{
    return strip(str, ::isspace);
}

/// Return an uppercased copy of str
inline std::string upper(const std::string& str)
{
    std::string res;
    res.reserve(str.size());
    for (std::string::const_iterator i = str.begin(); i != str.end(); ++i)
        res += ::toupper(*i);
    return res;
}

/// Return a lowercased copy of str
inline std::string lower(const std::string& str)
{
    std::string res;
    res.reserve(str.size());
    for (std::string::const_iterator i = str.begin(); i != str.end(); ++i)
        res += ::tolower(*i);
    return res;
}

/// Given a pathname, return the file name without its path
std::string basename(const std::string& pathname);

/// Given a pathname, return the directory name without the file name
std::string dirname(const std::string& pathname);

/// Append path2 to path1, adding slashes when appropriate
void appendpath(std::string& dest, const char* path2);

/// Append path2 to path1, adding slashes when appropriate
void appendpath(std::string& dest, const std::string& path2);

/// Append an arbitrary number of path components to \a dest
template<typename S1, typename S2, typename... Args>
void appendpath(std::string& dest, S1 first, S2 second, Args... next)
{
    appendpath(dest, first);
    appendpath(dest, second, next...);
}

/// Join two or more paths, adding slashes when appropriate
template<typename... Args>
std::string joinpath(Args... components)
{
    std::string res;
    appendpath(res, components...);
    return res;
}

/**
 * Normalise a pathname.
 *
 * For example, A//B, A/./B and A/foo/../B all become A/B.
 */
std::string normpath(const std::string& pathname);

/**
 * Split a string where a given substring is found
 *
 * This does a similar work to the split functions of perl, python and ruby.
 *
 * Example code:
 * \code
 *   str::Split splitter(my_string, "/");
 *   vector<string> split;
 *   std::copy(splitter.begin(), splitter.end(), back_inserter(split));
 * \endcode
 */
struct Split
{
    /// String to split
    std::string str;
    /// Separator
    std::string sep;
    /**
     * If true, skip empty tokens, effectively grouping consecutive separators
     * as if they were a single one
     */
    bool skip_empty;

    Split(const std::string& str, const std::string& sep, bool skip_empty=false)
        : str(str), sep(sep), skip_empty(skip_empty) {}

    class const_iterator : public std::iterator<std::input_iterator_tag, std::string>
    {
    protected:
        const Split* split = nullptr;
        /// Current token
        std::string cur;
        /// Position of the first character of the next token
        size_t end = 0;

        /// Move end past all the consecutive separators that start at its position
        void skip_separators();

    public:
        /// Begin iterator
        const_iterator(const Split& split);
        /// End iterator
        const_iterator() {}
        ~const_iterator();

        const_iterator& operator++();
        const std::string& operator*() const;
        const std::string* operator->() const;

        std::string remainder() const;

        bool operator==(const const_iterator& ti) const;
        bool operator!=(const const_iterator& ti) const;
    };

    /// Return the begin iterator to split a string on instances of sep
    const_iterator begin() { return const_iterator(*this); }

    /// Return the end iterator to string split
    const_iterator end() { return const_iterator(); }
};

/**
 * Escape the string so it can safely used as a C string inside double quotes
 */
std::string encode_cstring(const std::string& str);

/**
 * Unescape a C string, stopping at the first double quotes or at the end of
 * the string.
 *
 * lenParsed is set to the number of characters that were pased (which can be
 * greather than the size of the resulting string in case escapes were found)
 */
std::string decode_cstring(const std::string& str, size_t& lenParsed);

/// Urlencode a string
std::string encode_url(const std::string& str);

/// Decode an urlencoded string
std::string decode_url(const std::string& str);

/// Encode a string in Base64
std::string encode_base64(const std::string& str);

/// Decode a string encoded in Base64
std::string decode_base64(const std::string& str);

}
}
}
#endif
