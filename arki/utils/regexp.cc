#include "regexp.h"
#include <sstream>

using namespace std;

namespace arki {
namespace utils {

////// wibble::exception::Regexp

namespace {

std::string format_re_msg(const regex_t& re, int code, const string& user_msg)
{
    int size = 64;
    char* msg = new char[size];
    int nsize = regerror(code, &re, msg, size);
    if (nsize > size)
    {
        delete[] msg;
        msg = new char[nsize];
        regerror(code, &re, msg, nsize);
    }
    string m_message = user_msg + ": " + msg;
    delete[] msg;
    return m_message;
}

}

RegexpError::RegexpError(const regex_t& re, int code, const string& msg)
    : std::runtime_error(format_re_msg(re, code, msg))
{
}

////// Regexp

Regexp::Regexp(const string& expr, int match_count, int flags)
    : pmatch(0), nmatch(match_count)
{
    if (match_count == 0)
        flags |= REG_NOSUB;

    int res = regcomp(&re, expr.c_str(), flags);
    if (res)
        throw RegexpError(re, res, "cannot compile regexp \"" + expr + "\"");

    if (match_count > 0)
        pmatch = new regmatch_t[match_count];
}

Regexp::~Regexp() throw ()
{
	regfree(&re);
	if (pmatch)
		delete[] pmatch;
}

bool Regexp::match(const char* str, int flags)
{
    int res;

    if (nmatch)
    {
        res = regexec(&re, str, nmatch, pmatch, flags);
        lastMatch = str;
    }
    else
        res = regexec(&re, str, 0, 0, flags);

    switch (res)
    {
        case 0: return true;
        case REG_NOMATCH: return false;
        default: throw RegexpError(re, res, "cannot match string \"" + string(str) + "\"");
    }
}

bool Regexp::match(const string& str, int flags)
{
    int res;

	if (nmatch)
	{
		res = regexec(&re, str.c_str(), nmatch, pmatch, flags);
		lastMatch = str;
	}
	else
		res = regexec(&re, str.c_str(), 0, 0, flags);

    switch (res)
    {
        case 0: return true;
        case REG_NOMATCH: return false;
        default: throw RegexpError(re, res, "cannot match string \"" + str + "\"");
    }
}

string Regexp::operator[](int idx)
{
    if (idx >= nmatch)
    {
        stringstream ss;
        ss << "cannot get submatch of regexp: index " << idx << " out of range 0--" << nmatch;
        throw std::runtime_error(ss.str());
    }

    if (pmatch[idx].rm_so == -1)
        return string();

    return string(lastMatch, pmatch[idx].rm_so, pmatch[idx].rm_eo - pmatch[idx].rm_so);
}

size_t Regexp::matchStart(int idx)
{
    if (idx > nmatch)
    {
        stringstream ss;
        ss << "cannot get submatch of regexp: index " << idx << " out of range 0--" << nmatch;
        throw std::runtime_error(ss.str());
    }
    return pmatch[idx].rm_so;
}

size_t Regexp::matchEnd(int idx)
{
    if (idx > nmatch)
    {
        stringstream ss;
        ss << "cannot get submatch of regexp: index " << idx << " out of range 0--" << nmatch;
        throw std::runtime_error(ss.str());
    }
    return pmatch[idx].rm_eo;
}

size_t Regexp::matchLength(int idx)
{
    if (idx > nmatch)
    {
        stringstream ss;
        ss << "cannot get submatch of regexp: index " << idx << " out of range 0--" << nmatch;
        throw std::runtime_error(ss.str());
    }
    return pmatch[idx].rm_eo - pmatch[idx].rm_so;
}

Tokenizer::const_iterator& Tokenizer::const_iterator::operator++()
{
	// Skip past the last token
	beg = end;

	if (tok.re.match(tok.str.substr(beg)))
	{
		beg += tok.re.matchStart(0);
		end = beg + tok.re.matchLength(0);
	} 
	else
		beg = end = tok.str.size();

	return *this;
}

Splitter::const_iterator& Splitter::const_iterator::operator++()
{
	if (re.match(next))
	{
		if (re.matchLength(0))
		{
			cur = next.substr(0, re.matchStart(0));
			next = next.substr(re.matchStart(0) + re.matchLength(0));
		}
		else
		{
			if (!next.empty())
			{
				cur = next.substr(0, 1);
				next = next.substr(1);
			} else {
				cur = next;
			}
		}
	} else {
		cur = next;
		next = string();
	}
	return *this;
}

}
}
