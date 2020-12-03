#ifndef ARKI_METADATA_TESTS_H
#define ARKI_METADATA_TESTS_H

#include <arki/types/tests.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>

namespace arki {
namespace tests {

struct TestData
{
    std::string format;
    metadata::TestCollection mds;
    TestData(const std::string& format);
};

struct GRIBData : TestData
{
    GRIBData();
};

struct BUFRData : TestData
{
    BUFRData();
};

struct VM2Data : TestData
{
    VM2Data();
};

struct ODIMData : TestData
{
    ODIMData();
};

struct NCData : TestData
{
    NCData();
};

std::shared_ptr<Metadata> make_large_mock(const std::string& format, size_t size, unsigned month, unsigned day, unsigned hour=0);

void fill(Metadata& md);

struct ActualMetadata : public arki::utils::tests::Actual<const Metadata&>
{
    ActualMetadata(const Metadata& s) : Actual<const Metadata&>(s) {}

    void operator==(const Metadata& expected) const;
    void operator!=(const Metadata& expected) const;

    /// Check that a metadata field has the expected value
    void contains(const std::string& field, const std::string& expected);

    /// Check that a metadata field has the expected value
    void not_contains(const std::string& field, const std::string& expected);

    /// Check that the two metadata are the same, except for source and notes
    void is_similar(const Metadata& expected);

    /// Check that the metadata does contain an item of the given type
    void is_set(const std::string& field);

    /// Check that the metadata does contain an item of the given type
    void is_not_set(const std::string& field);

    /// Check if the metadata matches the given expression
    void matches(const std::string& expr);

    /// Check if the metadata does not match the given expression
    void not_matches(const std::string& expr);
};

inline arki::tests::ActualMetadata actual(const arki::Metadata& actual) { return arki::tests::ActualMetadata(actual); }

}
}

#endif
