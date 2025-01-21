#ifndef ARKI_TYPES_SOURCE_URL_H
#define ARKI_TYPES_SOURCE_URL_H

#include <arki/types/source.h>

namespace arki {
namespace types {
namespace source {

class URL : public Source
{
public:
    constexpr static const char* name = "URL";
    constexpr static const char* doc = R"(
The data is stored at a remote location.

This is a string containing a URL that points at the data remotely.

At the moment, remotely accessing a single data element is not supported, and
this field is usually only filled with the URL of the remote dataset that
contains the data.

Fetching remote data is usually done transparently and more efficiently at
query time, with data returned inline after the metadata.
)";
    std::string url;

    Style style() const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;

    int compare_local(const Source& o) const override;
    bool equals(const Type& o) const override;
    URL* clone() const override;

    static std::unique_ptr<URL> create(DataFormat format, const std::string& url);
    static std::unique_ptr<URL> decode_structure(const structured::Keys& keys, const structured::Reader& reader);
};


}
}
}
#endif
