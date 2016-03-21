#ifndef ARKI_UTILS_REGEXP_H
#define ARKI_UTILS_REGEXP_H

/// OO wrapper for regular expression functions

#include <sys/types.h>
#include <regex.h>
#include <stdexcept>
#include <cstddef>

namespace arki {
namespace utils {

class RegexpError : public std::runtime_error
{
public:
    RegexpError(const regex_t& re, int code, const std::string& msg);
};

class Regexp
{
protected:
	regex_t re;
	regmatch_t* pmatch;
	int nmatch;
	std::string lastMatch;

public:
    /* Note that match_count is required to be >1 to enable
       sub-regexp capture. The maximum *INCLUDES* the whole-regexp
       match (indexed 0). [TODO we may want to fix this to be more
       friendly?] */
    Regexp(const std::string& expr, int match_count = 0, int flags = 0);
    ~Regexp() throw ();

    bool match(const std::string& str, int flags = 0);

    /* Indexing is from 1 for capture matches, like perl's $0,
       $1... 0 is whole-regexp match, not a capture. TODO
       the range is miscalculated (an off-by-one, wrt. the
       counterintuitive match counting). */
    std::string operator[](int idx);

    size_t matchStart(int idx);
    size_t matchEnd(int idx);
    size_t matchLength(int idx);
};

class ERegexp : public Regexp
{
public:
    ERegexp(const std::string& expr, int match_count = 0, int flags = 0)
        : Regexp(expr, match_count, flags | REG_EXTENDED) {}
};

class Tokenizer
{
    const std::string& str;
    Regexp re;

public:
	class const_iterator
	{
		Tokenizer& tok;
		size_t beg, end;
	public:
		typedef std::string value_type;
		typedef ptrdiff_t difference_type;
		typedef value_type *pointer;
		typedef value_type &reference;
		typedef std::forward_iterator_tag iterator_category;

		const_iterator(Tokenizer& tok) : tok(tok), beg(0), end(0) { operator++(); }
		const_iterator(Tokenizer& tok, bool) : tok(tok), beg(tok.str.size()), end(tok.str.size()) {}

		const_iterator& operator++();

		std::string operator*() const
		{
			return tok.str.substr(beg, end-beg);
		}
		bool operator==(const const_iterator& ti) const
		{
			return beg == ti.beg && end == ti.end;
		}
		bool operator!=(const const_iterator& ti) const
		{
			return beg != ti.beg || end != ti.end;
		}
	};

	Tokenizer(const std::string& str, const std::string& re, int flags)
		: str(str), re(re, 1, flags) {}

	const_iterator begin() { return const_iterator(*this); }
	const_iterator end() { return const_iterator(*this, false); }
};

/**
 * Split a string using a regular expression to match the token separators.
 *
 * This does a similar work to the split functions of perl, python and ruby.
 *
 * Example code:
 * \code
 *   utils::Splitter splitter("[ \t]*,[ \t]*", REG_EXTENDED);
 *   vector<string> split;
 *   std::copy(splitter.begin(myString), splitter.end(), back_inserter(split));
 * \endcode
 *
 */
class Splitter
{
	Regexp re;

public:
    /**
     * Warning: the various iterators reuse the Regexps and therefore only one
     * iteration of a Splitter can be done at a given time.
     */
    // TODO: add iterator_traits
    class const_iterator
    {
        Regexp& re;
        std::string cur;
        std::string next;

	public:
		typedef std::string value_type;
		typedef ptrdiff_t difference_type;
		typedef value_type *pointer;
		typedef value_type &reference;
		typedef std::forward_iterator_tag iterator_category;

        const_iterator(Regexp& re, const std::string& str) : re(re), next(str) { ++*this; }
        const_iterator(Regexp& re) : re(re) {}

		const_iterator& operator++();

		const std::string& operator*() const
		{
			return cur;
		}
		const std::string* operator->() const
		{
			return &cur;
		}
		bool operator==(const const_iterator& ti) const
		{
			return cur == ti.cur && next == ti.next;
		}
		bool operator!=(const const_iterator& ti) const
		{
			return cur != ti.cur || next != ti.next;
		}
	};

	/**
	 * Create a splitter that uses the given regular expression to find tokens.
	 */
	Splitter(const std::string& re, int flags)
		: re(re, 1, flags) {}

	/**
	 * Split the string and iterate the resulting tokens
	 */
	const_iterator begin(const std::string& str) { return const_iterator(re, str); }
	const_iterator end() { return const_iterator(re); }
};

}
}
#endif
