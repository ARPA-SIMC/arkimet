%{
struct LexInterval {
	int val;
	int idx;
};

#include "config.h"
#include "parser.h"
#include "arki/wibble/grcal/grcal.h"
#include "reftime-parse.hh"
#include <string>
#include <ctype.h>

using namespace std;

struct Parser
{
	const std::string& str;
	std::string::const_iterator cur;

	Parser(const std::string& str) : str(str), cur(str.begin()) {}

	void error(const std::string& msg)
	{
		string lead = str.substr(0, cur - str.begin());
		string trail = str.substr(cur - str.begin());
        throw std::runtime_error("cannot parse reftime match expression \"" + lead + "[HERE]" + trail + "\": " + msg);
	}

	void eatNonSpaces()
	{
		while (!isspace(*cur))
			++cur;
	}

	void eatSpaces()
	{
		while (isspace(*cur))
			++cur;
	}

	char itype()
	{
		switch (*cur)
		{
			case 'h': {
				string name(cur, str.end());
				if (name == "h" || name == "hour" || name == "hours")
					return 'h';
				error("expected h, hour or hours");
				break;
			}
			case 'm': {
				string name(cur, str.end());
				if (name == "m" || name == "min" || name == "minute" || name == "minutes")
					return 'm';
				if (name == "month" || name == "months")
					return 'M';
				error("expected m, min, minute, minutes, month or months");
				break;
			}
			case 's': {
				string name(cur, str.end());
				if (name == "s" || name == "sec" || name == "second" || name == "seconds")
					return 's';
				error("expected s, sec, second or seconds");
				break;
			}
			case 'w': {
				string name(cur, str.end());
				if (name == "w" || name == "week" || name == "weeks")
					return 'w';
				error("expected w, week or weeks");
				break;
			}
			case 'd': {
				string name(cur, str.end());
				if (name == "d" || name == "day" || name == "days")
					return 'd';
				error("expected d, day or days");
				break;
			}
			case 'y': {
				string name(cur, str.end());
				if (name == "y" || name == "year" || name == "years")
					return 'y';
				error("expected y, year or years");
				break;
			}
		}
		error("expected a time name like hour, minute, second, day, week, month or year");
		// This is just to appease the compiler warnings
		return 0;
	}

	void eatInsensitive(const char* s)
	{
		if (*s == 0) return;
		if (cur == str.end() || ::tolower(*cur) != s[0])
			error(string("expecting ") + s);
		++cur;
		eatInsensitive(s+1);
	}
};

/**
 * Parser for Easter times
 */
struct EParser : public Parser
{
    int* res;

    EParser(const std::string& str, int* res) : Parser(str), res(res)
    {
        if (str.size() < 4)
            error("expecting at least 4 characters");
        // Parse the year directly
        res[0] = strtoul(str.substr(str.size() - 4).c_str(), 0, 10);
        wibble::grcal::date::easter(res[0], &res[1], &res[2]);
        res[3] = res[4] = res[5] = -1;
    }
};

/**
 * Simple recursive descent parser for reftimes
 */
struct DTParser : public Parser
{
	int* res;
	int res_cur;

	DTParser(const std::string& str, int* res) : Parser(str), res(res), res_cur(0) {}

	void getDate()
	{
		// Year
		num();
		// Month
		if (!eat('-')) return;
		num();
		// Day
		if (!eat('-')) return;
		num();
		// Eat optional 'T' at the end of the date
		if (cur != str.end() && (*cur == 'T' || *cur == 't')) ++cur;
		// Eat optional spaces
		eatSpaces();
		// Hour
		if (cur == str.end()) return;
		num();
		getTime();
	}
	void getTime()
	{
		// Minute
		if (!eat(':')) return;
		num();
		// Second
		if (!eat(':')) return;
		num();
	}
	void num()
	{
		string val;
		for ( ; cur != str.end() && isdigit(*cur); ++cur)
			val += *cur;
		if (val.empty())
			error("number expected");
		res[res_cur++] = strtoul(val.c_str(), 0, 10);
	}
	bool eat(char c)
	{
		if (cur == str.end() || *cur != c)
			return false;
		++cur;
		return true;
		//error(string("expected '") + c + "'");
	}
};

struct DParser : public DTParser
{
	DParser(const std::string& str, int* res) : DTParser(str, res)
	{
		this->res = res;
		for (int i = 0; i < 6; ++i)
			res[i] = -1;
		getDate();
		if (cur != str.end())
			error("trailing characters found");
	}
};

struct TParser : public DTParser
{
	TParser(const std::string& str, int* res) : DTParser(str, res)
	{
		this->res = res;
		for (int i = 0; i < 6; ++i)
			res[i] = -1;
		// Hour
		num();
		getTime();
		if (cur != str.end() && (*cur == 'z' || *cur == 'Z'))
			++cur;
		if (cur != str.end())
			error("trailing characters found");
	}
};

struct ISParser : public Parser
{
	struct LexInterval& res;

	ISParser(const std::string& str, struct LexInterval& res) : Parser(str), res(res)
	{
	}

	unsigned long int num()
	{
		string val;
		for ( ; cur != str.end() && isdigit(*cur); ++cur)
			val += *cur;
		if (val.empty())
			error("number expected");
		return strtoul(val.c_str(), 0, 10);
	}
};

struct IParser : public ISParser
{
	IParser(const std::string& str, struct LexInterval& res) : ISParser(str, res)
	{
		if (cur == str.end())
			error("number or 'a' expected");
		if (*cur == 'a' || *cur == 'A')
		{
			res.val = 1;
			eatNonSpaces();
		} else {
			res.val = num();
		}
		eatSpaces();
		itype();
	}

	void itype()
	{
		switch (*cur)
		{
			case 'h': {
				string name(cur, str.end());
				if (name == "h" || name == "hour" || name == "hours")
				{
					res.idx = 3;
					return;
				}
				error("expected h, hour or hours");
				break;
			}
			case 'm': {
				string name(cur, str.end());
				if (name == "m" || name == "min" || name == "minute" || name == "minutes")
				{
					res.idx = 4;
					return;
				}
				if (name == "month" || name == "months")
				{
					res.idx = 1;
					return;
				}
				error("expected m, min, minute, minutes, month or months");
				break;
			}
			case 's': {
				string name(cur, str.end());
				if (name == "s" || name == "sec" || name == "second" || name == "seconds")
				{
					res.idx = 5;
					return;
				}
				error("expected s, sec, second or seconds");
				break;
			}
			case 'w': {
				string name(cur, str.end());
				if (name == "w" || name == "week" || name == "weeks")
				{
					res.val *= 7;
					res.idx = 2;
					return;
				}
				error("expected w, week or weeks");
				break;
			}
			case 'd': {
				string name(cur, str.end());
				if (name == "d" || name == "day" || name == "days")
				{
					res.idx = 2;
					return;
				}
				error("expected d, day or days");
				break;
			}
			case 'y': {
				string name(cur, str.end());
				if (name == "y" || name == "year" || name == "years")
				{
					res.idx = 0;
					return;
				}
				error("expected y, year or years");
				break;
			}
		}
		error("expected a time name like hour, minute, second, day, week, month or year");
	}
};

struct SParser : public ISParser
{
	SParser(const std::string& str, struct LexInterval& res) : ISParser(str, res)
	{
		if (cur == str.end())
			error("expecting time step");
		if (*cur == '%')
			++cur;
		else
			eatInsensitive("every");
		eatSpaces();
		res.val = num();
		eatSpaces();
		itype();
	}

	void itype()
	{
		switch (*cur)
		{
			case 'h': {
				string name(cur, str.end());
				if (name == "h" || name == "hour" || name == "hours")
				{
					res.idx = 3;
					return;
				}
				error("expected h, hour or hours");
				break;
			}
			case 'm': {
				string name(cur, str.end());
				if (name == "m" || name == "min" || name == "minute" || name == "minutes")
				{
					res.idx = 4;
					return;
				}
				error("expected m, min, minute, minutes");
				break;
			}
			case 's': {
				string name(cur, str.end());
				if (name == "s" || name == "sec" || name == "second" || name == "seconds")
				{
					res.idx = 5;
					return;
				}
				error("expected s, sec, second or seconds");
				break;
			}
		}
		error("expected a time name like hours, minutes or seconds");
	}
};

#pragma GCC diagnostic ignored "-Wsign-compare"

%}

%option case-insensitive
%option 8bit
%option batch
%option bison-bridge
%option reentrant
%option prefix="arki_reftime"
%option outfile="reftime-lex.cc"
%option fast
%option noyywrap nounput
%option header-file="reftime-lex.h"

space   [ \t]+
unit    (h|hour|hours|m|min|minute|minutes|s|sec|second|seconds|w|week|weeks|d|da|day|days|month|months|y|year|years)
timeunit  (h|hour|hours|m|min|minute|minutes|s|sec|second|seconds)

%%

[0-9]{4}(-[0-9]{1,2}(-[0-9]{1,2}T?)?)?  {
		DParser p(std::string(yytext, yyleng), yylval->dtspec);
		return DATE;
}
[0-9]{1,2}(:[0-9]{2}(:[0-9]{2}Z?)?)?  {
		TParser p(std::string(yytext, yyleng), yylval->tspec);
		return TIME;
}
[0-9]+{space}?{unit} {
		IParser p(std::string(yytext, yyleng), yylval->lexInterval);
		return INTERVAL;
}
an?{space}{unit} {
		IParser p(std::string(yytext, yyleng), yylval->lexInterval);
		return INTERVAL;
}
(%|every){space}?[0-9]+{space}?{timeunit} {
		SParser p(std::string(yytext, yyleng), yylval->lexInterval);
		return STEP;
}
easter{space}?[0-9]{4} {
			  EParser p(std::string(yytext, yyleng), yylval->dtspec);
			  return DATE;
}
(processione{space})?san{space}?luca{space}?[0-9]{4} {
              // Compute easter
              EParser p(std::string(yytext, yyleng), yylval->dtspec);
              // Add 5 weeks - 1 day
              yylval->dtspec[2] += 35-1;
              // Lowerbound
              yylval->dtspec[3] = yylval->dtspec[4] = yylval->dtspec[5] = 0;
              // Normalise
              wibble::grcal::date::normalise(yylval->dtspec);
              // Put missing values back
              yylval->dtspec[3] = yylval->dtspec[4] = yylval->dtspec[5] = -1;
              return DATE;
}
now         { return NOW; }
today       { return TODAY; }
yesterday   { return YESTERDAY; }
tomorrow    { return TOMORROW; }
ago         { return AGO; }
before      { return BEFORE; }
after       { return AFTER; }
and         { return AND; }
\+          { return PLUS; }
\-          { return MINUS; }
midday      { return MIDDAY; }
noon        { return NOON; }
midnight    { return MIDNIGHT; }

from		{ return GE; }
\>=			{ return GE; }
\>			{ return GT; }
until		{ return LE; }
\<=			{ return LE; }
\<			{ return LT; }
==?			{ return EQ; }

,			{ return COMMA; }

{space}     { /* space */ }

.			{ yylval->error = yytext[0]; return UNEXPECTED; }

%%

/* vim:set syntax=lex ts=4 sw=4: */
