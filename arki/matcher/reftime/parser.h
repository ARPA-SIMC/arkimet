#ifndef ARKI_MATCHER_REFTIME_PARSER_H
#define ARKI_MATCHER_REFTIME_PARSER_H

#include <arki/types.h>
#include <arki/core/fuzzytime.h>
#include <arki/utils/string.h>
#include <string>
#include <ctime>
#include <vector>
#include <set>
#include <sstream>
#include <cstdio>

namespace arki {
namespace matcher {
namespace reftime {

struct DTMatch
{
    virtual ~DTMatch() {}
    virtual bool match(const core::Time& tt) const = 0;
    virtual bool match(const core::Time& begin, const core::Time& end) const = 0;
    virtual std::string sql(const std::string& column) const = 0;
    virtual std::string toString() const = 0;

    /**
     * Time (in seconds since midnight) of this expression, used as a reference
     * when building derived times. For example, an expression of
     * "reftime:>yesterday every 12h" may become ">=yyyy-mm-dd 23:59:59" but
     * the step should still match 00:00 and 12:00, and not 23:59 and 11:59
     */
    virtual int timebase() const = 0;
    virtual bool isLead() const { return true; }

    /**
     * Restrict a datetime range, returning the new range endpoints in begin
     * and end.
     *
     * A NULL unique_ptr means an open end.
     *
     * Returns true if the result is a valid interval, false if this match does
     * not match the given interval at all.
     */
    virtual bool restrict_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end) const = 0;

    static DTMatch* createLE(core::FuzzyTime* tt);
    static DTMatch* createLT(core::FuzzyTime* tt);
    static DTMatch* createGE(core::FuzzyTime* tt);
    static DTMatch* createGT(core::FuzzyTime* tt);
    static DTMatch* createEQ(core::FuzzyTime* tt);

    static DTMatch* createTimeLE(const int* tt);
    static DTMatch* createTimeLT(const int* tt);
    static DTMatch* createTimeGE(const int* tt);
    static DTMatch* createTimeGT(const int* tt);
    static DTMatch* createTimeEQ(const int* tt);
};

struct Parser
{
    time_t tnow;
    std::vector<std::string> errors;
    std::string unexpected;

    std::vector<DTMatch*> res;

    Parser()
    {
        tnow = time(NULL);
    }
    ~Parser()
    {
        for (auto& i: res) delete i;
    }

	void add(DTMatch* t)
	{
		//fprintf(stderr, "ADD %s\n", t->toString().c_str());
		res.push_back(t);
	}

    void add_step(int val, int idx, DTMatch* base=0);

    void parse(const std::string& str);

    arki::core::FuzzyTime* mknow();
    arki::core::FuzzyTime* mktoday();
    arki::core::FuzzyTime* mkyesterday();
    arki::core::FuzzyTime* mktomorrow();
};

}
}
}
#endif
