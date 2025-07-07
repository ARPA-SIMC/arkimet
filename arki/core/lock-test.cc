#include "arki/tests/tests.h"
#include "lock.h"

using namespace std;
using namespace arki;
using namespace arki::core;
using namespace arki::utils;
using namespace arki::tests;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
};

Tests test("arki_core_lock");

std::shared_ptr<core::ReadLock> make_read()
{
    return std::make_shared<core::lock::FileReadLock>("lockfile",
                                                      lock::policy_ofd);
}

std::shared_ptr<core::AppendLock> make_append()
{
    return std::make_shared<core::lock::FileAppendLock>("lockfile",
                                                        lock::policy_ofd);
}

std::shared_ptr<core::CheckLock> make_check()
{
    return std::make_shared<core::lock::FileCheckLock>("lockfile",
                                                       lock::policy_ofd);
}

void Tests::register_tests()
{

    add_method("reader_reader", [] {
        core::lock::TestNowait lock_nowait;
        auto lock1 = make_read();
        auto lock2 = make_read();
    });

    add_method("reader_append", [] {
        core::lock::TestNowait lock_nowait;
        {
            auto lock1 = wcallchecked(make_read());
            auto lock2 = wcallchecked(make_append());
        }
        {
            auto lock1 = wcallchecked(make_append());
            auto lock2 = wcallchecked(make_read());
        }
    });

    add_method("reader_checkro", [] {
        core::lock::TestNowait lock_nowait;
        {
            auto lock1 = make_read();
            auto lock2 = make_check();
        }
        {
            auto lock1 = make_check();
            auto lock2 = make_read();
        }
    });

    add_method("reader_checkrw", [] {
        core::lock::TestNowait lock_nowait;
        {
            auto lock1 = make_read();
            auto lock2 = make_check();
            try
            {
                lock2->write_lock();
            }
            catch (lock::locked_error& e)
            {
                wassert(actual(e.what()).contains("already held"));
            }
        }

        {
            auto lock1  = make_check();
            auto lock1a = lock1->write_lock();
            try
            {
                make_read();
            }
            catch (lock::locked_error& e)
            {
                wassert(actual(e.what()).contains("already held"));
            }
        }
    });

    add_method("append_append", [] {
        core::lock::TestNowait lock_nowait;
        auto lock = make_append();
        try
        {
            make_append();
        }
        catch (lock::locked_error& e)
        {
            wassert(actual(e.what()).contains("already held"));
        }
    });

    add_method("append_checkro", [] {
        core::lock::TestNowait lock_nowait;
        {
            auto lock = make_append();
            try
            {
                make_check();
            }
            catch (lock::locked_error& e)
            {
                wassert(actual(e.what()).contains("already held"));
            }
        }

        {
            auto lock = make_check();
            try
            {
                make_append();
            }
            catch (lock::locked_error& e)
            {
                wassert(actual(e.what()).contains("already held"));
            }
        }
    });

    add_method("append_checkrw", [] {
        core::lock::TestNowait lock_nowait;
        auto lock   = make_check();
        auto lock1a = lock->write_lock();
        try
        {
            make_append();
        }
        catch (lock::locked_error& e)
        {
            wassert(actual(e.what()).contains("already held"));
        }
    });

    add_method("checkro_checkro", [] {
        core::lock::TestNowait lock_nowait;
        auto lock = make_check();
        try
        {
            make_check();
        }
        catch (lock::locked_error& e)
        {
            wassert(actual(e.what()).contains("already held"));
        }
    });

    add_method("checkro_checkrw", [] {
        core::lock::TestNowait lock_nowait;
        auto lock   = make_check();
        auto lock1a = lock->write_lock();
        try
        {
            make_check();
        }
        catch (lock::locked_error& e)
        {
            wassert(actual(e.what()).contains("already held"));
        }
    });
}

} // namespace
