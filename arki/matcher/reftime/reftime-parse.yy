%{
#include "config.h"
#include "parser.h"
#include "arki/core/fuzzytime.h"
#include "arki/wibble/grcal/grcal.h"
#include <string>
#include <stdexcept>
#include <ctime>

using namespace std;
using namespace arki::matcher::reftime;

struct LexInterval {
	int val;
	int idx;
};

#include "reftime-parse.hh"
#include "reftime-lex.h"

//#define YYDEBUG 1

//#define DEBUG_PARSER

#ifdef DEBUG_PARSER
#include <stdio.h>

void pdate(const char* msg, const int* date)
{
	fprintf(stderr, "%s: %04d-%02d-%02d %02d:%02d:%02d\n", msg, date[0], date[1], date[2], date[3], date[4], date[5]);
}
#else
#define pdate(msg, date) do {} while(0)
#endif

static void arki_reftimeerror(yyscan_t yyscanner, arki::matcher::reftime::Parser& state, const char* s);

static inline void copy(int* dst, const int* src)
{
	for (int i = 0; i < 6; ++i)
		dst[i] = src[i];
}

static inline void init_interval(int* dst, int val, int idx)
{
	for (int i = 0; i < 6; ++i)
		if (i == idx)
			dst[i] = val;
		else
			dst[i] = 0;
}

static void interval_add(int* dst, const int* val, bool subtract = false)
{
	pdate("add", val);
	pdate(" add to", dst);

	// Compute what's the last valid item between dst and val
	int depth = 0;
	for (int i = 0; i < 6; ++i)
	{
		if (val[i] !=  0)
			depth = i;
		else if (dst[i] != -1)
			depth = i;
	}

	// Lowerbound dst, adding val values to it
	for (int i = 0; i < 6; ++i)
	{
		if (dst[i] == -1)
			dst[i] = (i == 1 || i == 2) ? 1 : 0;
		if (subtract)
			dst[i] -= val[i];
		else
			dst[i] += val[i];
	}
	pdate(" added", dst);

    // Normalise
    wibble::grcal::date::normalise(dst);
    pdate(" normalised", dst);

	// Restore all the values not defined in dst and val to -1
	for (int i = depth + 1; i < 6; ++i)
		dst[i] = -1;
	pdate(" readded missing", dst);
}

void interval_sub(int* dst, const int* val)
{
	interval_add(dst, val, true);
}

// Set dst to 'val' before 'now'
void interval_ago(int* dst, time_t& now, const int* val)
{
    struct tm t;
    gmtime_r(&now, &t);
    wibble::grcal::date::fromtm(t, dst);
    interval_sub(dst, val);

	// Compute how many nonzero items we have
	int depth = 0;
	for (int i = 0; i < 6; ++i)
		if (val[i])
			depth = i+1;

	// Set all the elements after the position of the last nonzero item to
	// -1
	for (int i = depth; i < 6; ++i)
		dst[i] = -1;
}


%}

%union {
	arki::core::FuzzyTime dtspec;
	int tspec[3];
	struct LexInterval lexInterval;
	int interval[6];
	struct DTMatch* dtmatch;
	char error;
}

%defines
%pure-parser
%name-prefix "arki_reftime"
%lex-param { yyscan_t yyscanner }
%parse-param { void* yyscanner }
%parse-param { arki::matcher::reftime::Parser& state }
//%error-verbose

%start Input
%token <dtspec> DATE
%token <tspec> TIME
%token <lexInterval> INTERVAL STEP
%token <error> UNEXPECTED
%token NOW TODAY YESTERDAY TOMORROW AGO BEFORE AFTER AND PLUS MINUS MIDDAY NOON MIDNIGHT
%token GE GT LE LT EQ COMMA
%type <dtspec> Absolute
%type <dtspec> Date
%type <tspec> Daytime
%type <interval> Interval
%type <void> Input
%type <dtmatch> DateExpr TimeExpr

%%

Input	: DateExpr			{ state.add($1); }
        | DateExpr STEP		{ state.add($1); state.add_step($2.val, $2.idx, $1); }
        | TimeExpr			{ state.add($1); }
        | TimeExpr STEP		{ state.add($1); state.add_step($2.val, $2.idx, $1); }
        | STEP				{ state.add_step($1.val, $1.idx); }
		| Input COMMA Input {}
		| error Unexpected  {
								string msg = "before '";
								msg += state.unexpected;
								msg += "'";
								arki_reftimeerror(yyscanner, state, msg.c_str());
								yyclearin;
								YYERROR;
							}
        ;

// Accumulate unexpected characters to generate nicer error messages
Unexpected: UNEXPECTED		{ state.unexpected = string() + $1; }
		 | Unexpected UNEXPECTED
		 					{ state.unexpected += $2; }
		 ;

DateExpr : GE Absolute		{ $$ = DTMatch::createGE($2); }
         | GT Absolute		{ $$ = DTMatch::createGT($2); }
		 | LE Absolute		{ $$ = DTMatch::createLE($2); }
		 | LT Absolute		{ $$ = DTMatch::createLT($2); }
		 | EQ Absolute		{ $$ = DTMatch::createEQ($2); }
		 ;

TimeExpr : GE Daytime		{ $$ = DTMatch::createTimeGE($2); }
         | GT Daytime		{ $$ = DTMatch::createTimeGT($2); }
		 | LE Daytime		{ $$ = DTMatch::createTimeLE($2); }
		 | LT Daytime		{ $$ = DTMatch::createTimeLT($2); }
		 | EQ Daytime		{ $$ = DTMatch::createTimeEQ($2); }
		 ;

Interval : INTERVAL						{ init_interval($$, $1.val, $1.idx); }
		 | Interval INTERVAL		    { copy($$, $1); $$[$2.idx] += $2.val; }
		 | Interval AND INTERVAL		{ copy($$, $1); $$[$3.idx] += $3.val; }
		 ;

Absolute : NOW							{ state.mknow($$); }
         | Date							{ copy($$, $1); }
         | Date Daytime					{ wibble::grcal::date::mergetime($1, $2, $$); }
         | Interval AGO					{ interval_ago($$, state.tnow, $1); }
         | Interval BEFORE Absolute		{ copy($$, $3); interval_sub($$, $1); }
         | Interval AFTER Absolute		{ copy($$, $3); interval_add($$, $1); }
         | Absolute PLUS Interval		{ copy($$, $1); interval_add($$, $3); }
         | Absolute MINUS Interval		{ copy($$, $1); interval_sub($$, $3); }
         ;

Date	 : TODAY						{ state.mktoday($$); }
	 	 | YESTERDAY					{ state.mkyesterday($$); }
	 	 | TOMORROW						{ state.mktomorrow($$); }
	 	 | DATE							{ }
	 	 ;

Daytime : MIDDAY						{ $$[0] = 12; $$[1] = 0; $$[2] = 0; }
        | NOON							{ $$[0] = 12; $$[1] = 0; $$[2] = 0; }
        | MIDNIGHT						{ $$[0] = 0; $$[1] = 0; $$[2] = 0; }
        | TIME							{ }
        ;

%%

void arki_reftimeerror(yyscan_t yyscanner, arki::matcher::reftime::Parser& state, const char* s)
{
	state.errors.push_back(s);
	//tagcoll::TagexprParser::instance()->addError(s);
	//fprintf(stderr, "%s\n", s);
}

namespace arki {
namespace matcher {
namespace reftime {

void Parser::parse(const std::string& str)
{
	for (vector<DTMatch*>::iterator i = res.begin(); i != res.end(); ++i)
		delete *i;
	res.clear();

	yyscan_t scanner;
	arki_reftimelex_init(&scanner);

	YY_BUFFER_STATE buf = arki_reftime_scan_string(str.c_str(), scanner);

	int res = yyparse(scanner, *this);
	arki_reftime_delete_buffer(buf, scanner);
	arki_reftimelex_destroy(scanner);

	switch (res)
	{
		case 0:
			// Parse successful
			break;
		case 1: {
            // Syntax error
            stringstream ss;
            ss << "cannot parse '" << str << "': ";
            for (vector<string>::const_iterator i = errors.begin();
                    i != errors.end(); ++i)
            {
                if (i != errors.begin())
                    ss << "; ";
                ss << *i;
            }
            throw std::runtime_error(ss.str());
		}
		case 2:
			// Out of memory
			throw std::runtime_error("parser out of memory");
        default: {
            // Should never happen
            stringstream ss;
            ss << "cannot parse '" << str << "': Bison parser function returned unexpected value " << res;
            throw std::runtime_error(ss.str());
        }
    }
}

}
}
}
