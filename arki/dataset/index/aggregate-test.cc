#include "arki/types/tests.h"
#include "arki/types/origin.h"
#include "arki/types/product.h"
#include "arki/metadata.h"
#include "arki/matcher.h"
#include "arki/types.h"
#include "aggregate.h"

namespace {
using namespace std;
using namespace arki;
using namespace arki::tests;
using namespace arki::types;
using namespace arki::dataset::index;

class Tests : public TestCase
{
    using TestCase::TestCase;
    void register_tests() override;
} test("arki_dataset_index_aggregate");

void Tests::register_tests() {

add_method("basic", [] {
    utils::sqlite::SQLiteDB db;
    db.open(":memory:");

	set<types::Code> members;
	members.insert(TYPE_ORIGIN);
	members.insert(TYPE_PRODUCT);
	members.insert(TYPE_LEVEL);
	Aggregate u(db, "test", members);
	u.initDB(members);

    Metadata md;
    unique_ptr<Type> origin(Origin::createGRIB1(200, 0, 0));
    unique_ptr<Type> product(Product::createGRIB1(200, 1, 2));

    wassert(actual(u.get(md)) == -1);
    md.set(*origin);
    md.set(*product);

    // ID is -1 if it is not in the database
    wassert(actual(u.get(md)) == -1);

    int id = u.obtain(md);
    wassert(actual(id) == 1);

    // Insert again, we should have the same result
    wassert(actual(u.obtain(md)) == 1);

    wassert(actual(u.get(md)) == 1);

    // Retrieve from the database
    Metadata md1;
    u.read(1, md1);
    wassert(actual_type(md1.get<types::Origin>()) == origin);
    wassert(actual_type(md1.get<types::Product>()) == product);

    Matcher m = Matcher::parse("origin:GRIB1,200; product:GRIB1,200,1");
    vector<string> constraints;
    wassert(actual(u.add_constraints(m, constraints, "t")) == 2);
    wassert(actual(constraints.size()) == 2u);
    wassert(actual(constraints[0]) == "t.origin =1");
    wassert(actual(constraints[1]) == "t.product =1");
});

}

}
