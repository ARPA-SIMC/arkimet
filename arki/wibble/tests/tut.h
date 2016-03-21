#include <arki/utils/tests.h>

/**
 * tut replacement that feeds tests to wobble-based tests
 */

namespace tut {

/**
* Walks through test tree and stores address of each
* test method in group. Instantiation stops at 0.
*/
template <class Test,class Group,int n>
struct tests_registerer
{
    static void reg(Group& group)
    {
        group.reg(n, &Test::template test<n>);
        tests_registerer<Test,Group,n-1>::reg(group);
    }
};

template<class Test,class Group>
struct tests_registerer<Test,Group,0>
{
    static void reg(Group&){};
};

/**
 * Test object. Contains data test run upon and default test method 
 * implementation. Inherited from Data to allow tests to  
 * access test data as members.
 */
template <class Data>
class test_object : public Data
{
public:
    /**
     * Default constructor
     */
    test_object(){};

    /**
     * The flag is set to true by default (dummy) test.
     * Used to detect usused test numbers and avoid unnecessary
     * test object creation which may be time-consuming depending
     * on operations described in Data::Data() and Data::~Data().
     * TODO: replace with throwing special exception from default test.
     */
    bool called_method_was_a_dummy_test_;

    /**
     * Default do-nothing test.
     */
    template<int n>
    void test()
    {
        called_method_was_a_dummy_test_ = true;
    }
};

template<class Data, int MaxTestsInGroup=50>
struct test_group : public arki::utils::tests::TestCase
{
    typedef test_object<Data> object;
    typedef void (test_object<Data>::*testmethod)();

    test_group(const std::string& name) : TestCase(name)
    {
    }

    void register_tests() override
    {
        // register all tests
        tests_registerer<object,test_group,MaxTestsInGroup>::reg(*this);
    }

    /**
     * Registers test method under given number.
     */
    void reg(int n,testmethod tm)
    {
        add_method(std::to_string(n), [tm] {
            test_object<Data> obj;
            (obj.*tm)();
        });
    }
};

}
