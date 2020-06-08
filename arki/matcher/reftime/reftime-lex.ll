%{
#include "config.h"
#include "parser.h"
#include "arki/core/fuzzytime.h"
#include "arki/matcher/reftime/lexer.h"
#include "reftime-parse.hh"
#include <string>
#include <ctype.h>

using namespace std;
using namespace arki::matcher::reftime::lexer;

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

space     [ \t]+
unit      (h|hour|hours|m|min|minute|minutes|s|sec|second|seconds|w|week|weeks|d|da|day|days|month|months|y|year|years)
timeunit  (h|hour|hours|m|min|minute|minutes|s|sec|second|seconds)
time      [0-9]{1,2}(:[0-9]{2}(:[0-9]{2}Z?)?)? 

%%

[0-9]{4}(-[0-9]{1,2}(-[0-9]{1,2}T?)?)?  {
        yylval->dtspec = parse_datetime(std::string(yytext, yyleng));
        return DATE;
}
{time} {
        parse_time(std::string(yytext, yyleng), yylval->tspec);
        return TIME;
}
[0-9]+{space}*{unit} {
        IParser p(std::string(yytext, yyleng), yylval->lexInterval);
        return INTERVAL;
}
an?{space}+{unit} {
        IParser p(std::string(yytext, yyleng), yylval->lexInterval);
        return INTERVAL;
}
@{time} {
        parse_time(std::string(yytext + 1, yyleng - 1), yylval->tspec);
        return TIMEBASE;
}
(%|every){space}*[0-9]+{space}*{timeunit} {
        SParser p(std::string(yytext, yyleng), yylval->lexInterval);
        return STEP;
}
easter{space}*[0-9]{4} {
        yylval->dtspec = parse_easter(std::string(yytext, yyleng));
        return DATE;
}
(processione{space}*)?san{space}*?luca{space}*?[0-9]{4} {
        // Compute easter
        yylval->dtspec = parse_easter(std::string(yytext, yyleng));
        // Lowerbound
        arki::core::Time lb = yylval->dtspec->lowerbound();
        // Add 5 weeks - 1 day
        lb.da += 35-1;
        lb.normalise();
        // Put missing values back
        *(yylval->dtspec) = lb;
        yylval->dtspec->ho = yylval->dtspec->mi = yylval->dtspec->se = -1;
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

from        { return GE; }
\>=         { return GE; }
\>          { return GT; }
until       { return LE; }
\<=         { return LE; }
\<          { return LT; }
==?         { return EQ; }

,           { return COMMA; }

{space}     { /* space */ }

.           { yylval->error = yytext[0]; return UNEXPECTED; }

%%
