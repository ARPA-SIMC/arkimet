#ifndef ARKI_METADATA_TESTS_H
#define ARKI_METADATA_TESTS_H

#include <arki/types/tests.h>
#include <arki/metadata.h>

namespace arki {
class Metadata;

namespace tests {

void fill(Metadata& md);

struct ActualMetadata : public arki::utils::tests::Actual<Metadata>
{
    ActualMetadata(const Metadata& s) : Actual<Metadata>(s) {}

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
