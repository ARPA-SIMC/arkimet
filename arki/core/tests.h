#ifndef ARKI_CORE_TESTS_H
#define ARKI_CORE_TESTS_H

#include <arki/core/time.h>
#include <arki/tests/tests.h>

namespace arki {
namespace tests {

class ActualTime : public arki::utils::tests::Actual<arki::core::Time>
{
public:
    ActualTime(const core::Time& actual)
        : arki::utils::tests::Actual<core::Time>(actual)
    {
    }

    using arki::utils::tests::Actual<arki::core::Time>::operator==;
    using arki::utils::tests::Actual<arki::core::Time>::operator!=;

    void operator==(const std::string& expected) const;
    void operator!=(const std::string& expected) const;

    /**
     * Check that a metadata field can be serialized and deserialized in all
     * sorts of ways
     */
    void serializes() const;

    /**
     * Check comparison operators
     */
    void compares(const core::Time& higher) const;

    /// Check all components of a Time item
    void is(int ye, int mo, int da, int ho = 0, int mi = 0, int se = 0);
};

inline arki::tests::ActualTime actual_time(const arki::core::Time& actual)
{
    return arki::tests::ActualTime(actual);
}
inline arki::tests::ActualTime actual(const arki::core::Time& actual)
{
    return arki::tests::ActualTime(actual);
}

void skip_unless_libzip();
void skip_unless_libarchive();
void skip_unless_grib();
void skip_unless_bufr();
void skip_unless_vm2();
void skip_unless_geos();
void skip_unless_filesystem_has_holes(const std::string& path);
void skip_unless_filesystem_has_ofd_locks(const std::string& path);
void skip_unless_splice();

void delete_if_exists(const std::string& name);

} // namespace tests
} // namespace arki
#endif
