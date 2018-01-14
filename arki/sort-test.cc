#include <arki/types/tests.h>
#include <arki/metadata/collection.h>
#include <arki/sort.h>
#include <arki/types/reftime.h>
#include <arki/types/run.h>
#include <arki/utils/string.h>

#include <sstream>
#include <iostream>

namespace std{
static ostream& operator<<(ostream& out, const vector<int>& v)
{
    return out << arki::utils::str::join(", ", v.begin(), v.end());
}
}

namespace {
using namespace std;
using namespace arki::tests;
using namespace arki;
using namespace arki::types;
using arki::core::Time;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_sort");

void produce(int hour, int minute, int run, sort::Stream& c)
{
    unique_ptr<Metadata> md(new Metadata);
    md->set(Reftime::createPosition(Time(2008, 7, 6, hour, minute, 0)));
    md->set(Run::createMinute(run));
    c.add(move(md));
}

vector<int> mdvals(const Metadata& md)
{
    const reftime::Position* rt = dynamic_cast<const reftime::Position*>(md.get<Reftime>());
    const run::Minute* run = dynamic_cast<const run::Minute*>(md.get<Run>());
    vector<int> res;
    res.push_back(rt->time.ho);
    res.push_back(rt->time.mi);
    res.push_back(run->minute()/60);
    return res;
}

vector<int> mdvals(int h, int m, int r)
{
	vector<int> res;
	res.push_back(h);
	res.push_back(m);
	res.push_back(r);
	return res;
}

void Tests::register_tests() {

// Test a simple case
add_method("simple", [] {
    metadata::Collection mdc;
    unique_ptr<sort::Compare> cmp = sort::Compare::parse("hour:run,-reftime");
    sort::Stream sorter(*cmp, mdc.inserter_func());

	produce(0, 0, 10, sorter);
	produce(0, 1, 9, sorter);
	produce(0, 3, 7, sorter);
	produce(0, 2, 9, sorter);
	produce(1, 0, 8, sorter);
	produce(1, 1, 9, sorter);
	produce(1, 2, 7, sorter);

	sorter.flush();

	ensure_equals(mdvals(mdc[0]), mdvals(0, 3, 7));
	ensure_equals(mdvals(mdc[1]), mdvals(0, 2, 9));
	ensure_equals(mdvals(mdc[2]), mdvals(0, 1, 9));
	ensure_equals(mdvals(mdc[3]), mdvals(0, 0, 10));
	ensure_equals(mdvals(mdc[4]), mdvals(1, 2, 7));
	ensure_equals(mdvals(mdc[5]), mdvals(1, 0, 8));
	ensure_equals(mdvals(mdc[6]), mdvals(1, 1, 9));
});

// An empty expression sorts by reference time
add_method("empty", [] {
    metadata::Collection mdc;
    unique_ptr<sort::Compare> cmp = sort::Compare::parse("");
    sort::Stream sorter(*cmp, mdc.inserter_func());

	produce(1, 0, 8, sorter);
	produce(1, 1, 9, sorter);
	produce(1, 2, 7, sorter);
	produce(0, 0, 10, sorter);
	produce(0, 1, 9, sorter);
	produce(0, 3, 7, sorter);
	produce(0, 2, 9, sorter);

	sorter.flush();

	ensure_equals(mdvals(mdc[0]), mdvals(0, 0, 10));
	ensure_equals(mdvals(mdc[1]), mdvals(0, 1, 9));
	ensure_equals(mdvals(mdc[2]), mdvals(0, 2, 9));
	ensure_equals(mdvals(mdc[3]), mdvals(0, 3, 7));
	ensure_equals(mdvals(mdc[4]), mdvals(1, 0, 8));
	ensure_equals(mdvals(mdc[5]), mdvals(1, 1, 9));
	ensure_equals(mdvals(mdc[6]), mdvals(1, 2, 7));
});

}

}
