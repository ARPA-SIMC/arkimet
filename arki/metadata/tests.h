#ifndef ARKI_METADATA_TESTS_H
#define ARKI_METADATA_TESTS_H

#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/types/tests.h>

namespace arki::tests {

struct TestData
{
    DataFormat format;
    metadata::TestCollection mds;
    explicit TestData(DataFormat format);
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

struct JPEGData : TestData
{
    JPEGData();
};

std::shared_ptr<Metadata> make_large_mock(DataFormat format, size_t size,
                                          unsigned month, unsigned day,
                                          unsigned hour = 0);

void fill(Metadata& md);

struct ActualMetadata : public arki::utils::tests::Actual<const Metadata&>
{
    explicit ActualMetadata(const Metadata& s) : Actual<const Metadata&>(s) {}

    void operator==(std::shared_ptr<Metadata> expected) const
    {
        return operator==(*expected);
    }
    void operator!=(std::shared_ptr<Metadata> expected) const
    {
        return operator!=(*expected);
    }

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

inline arki::tests::ActualMetadata actual(const arki::Metadata& actual)
{
    return arki::tests::ActualMetadata(actual);
}
inline arki::tests::ActualMetadata
actual(std::shared_ptr<const arki::Metadata> actual)
{
    return arki::tests::ActualMetadata(*actual);
}

} // namespace arki::tests

#endif
