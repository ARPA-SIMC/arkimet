#include "reader.h"
#include "arki/core/binary.h"
#include "arki/core/file.h"
#include "arki/metadata.h"
#include "arki/summary.h"
#include "arki/types/bundle.h"
#include "arki/types/source.h"
#include "arki/utils/compress.h"
#include "iotrace.h"
#include <sstream>
#include <string>

using namespace std::string_literals;

namespace arki::metadata {

namespace reader {

std::filesystem::path infer_basedir(const std::filesystem::path& path)
{
    try
    {
        return std::filesystem::canonical(path).parent_path();
    }
    catch (std::filesystem::filesystem_error&)
    {
        return std::filesystem::current_path();
    }
}

template <typename FileType>
BaseReader<FileType>::BaseReader(FileType& in)
    : in(in), basedir(infer_basedir(in.path()))
{
}

template <typename FileType>
BaseReader<FileType>::BaseReader(FileType& in,
                                 const std::filesystem::path& basedir)
    : in(in), basedir(basedir)
{
}

template <typename FileType>
bool BaseReader<FileType>::read_bundle(types::Bundle& bundle)
{
    if (size_t header_size = bundle.read_header(in))
        offset += header_size;
    else
        return false;

    // Ensure first 2 bytes are MD or !D
    if (bundle.signature != "MD" && bundle.signature != "!D" &&
        bundle.signature != "MG" && bundle.signature != "SU")
        throw std::runtime_error(in.path().native() +
                                 ": unsupported metadata signature '" +
                                 bundle.signature + "'");

    if (size_t data_size = bundle.read_data(in))
        offset += data_size;
    else
        return false;
    return true;
}

template <typename FileType>
bool BaseReader<FileType>::read_group(const types::Bundle& bundle,
                                      metadata_dest_func dest)
{
    // Handle metadata group
    if (bundle.version != 0)
    {
        std::stringstream ss;
        ss << in.path().native() << ": found version " << bundle.version
           << " but only version 0 (LZO compressed) is supported";
        throw std::runtime_error(ss.str());
    }

    core::BinaryDecoder dec(bundle.data);

    // Read uncompressed size
    uint32_t uncsize = dec.pop_uint(4, "uncompressed item size");

    std::vector<uint8_t> decomp =
        utils::compress::unlzo(dec.buf, dec.size, uncsize);
    dec.buf += dec.size;
    dec.size = 0;

    core::BinaryDecoder unenc(decomp);

    std::string isig;
    unsigned iver;
    while (unenc)
    {
        core::BinaryDecoder inner = unenc.pop_metadata_bundle(isig, iver);
        auto md = Metadata::read_binary_inner(inner, iver, in.path());
        if (!dest(move(md)))
            return false;
    }

    return true;
}

template <typename FileType>
bool BaseReader<FileType>::read_inline_data(Metadata& md)
{
    if (size_t inline_data_size = md.read_inline_data(in))
        offset += inline_data_size;
    else
        return false;
    return true;
}

template <typename FileType>
std::shared_ptr<Metadata>
BaseReader<FileType>::decode_metadata(const types::Bundle& bundle) const
{
    core::BinaryDecoder inner(bundle.data);
    return Metadata::read_binary_inner(
        inner, bundle.version, metadata::ReadContext(in.path(), basedir));
}

template <typename FileType>
void BaseReader<FileType>::decode_summary(const types::Bundle& bundle,
                                          Summary& summary) const
{
    arki::core::BinaryDecoder inner(bundle.data);
    summary.read_inner(inner, bundle.version, in.path());
}

template <typename FileType>
std::shared_ptr<Metadata> BaseReader<FileType>::read(bool read_inline)
{
    types::Bundle bundle;
    if (!read_bundle(bundle))
        return std::shared_ptr<Metadata>();

    // Ensure first 2 bytes are MD or !D
    if (bundle.signature != "MD")
        throw std::runtime_error(in.path().native() +
                                 ": metadata entry does not start with 'MD'");

    auto md = decode_metadata(bundle);

    // If the source is inline, then the data follows the metadata
    if (read_inline && md->source().style() == types::Source::Style::INLINE)
        read_inline_data(*md);

    return md;
}

template <typename FileType>
bool BaseReader<FileType>::read_all(metadata_dest_func dest)
{
    types::Bundle bundle;
    while (read_bundle(bundle))
    {
        if (bundle.signature == "MG")
        {
            // Handle metadata group
            iotrace::trace_file(in.path(), 0, 0, "read metadata group");
            if (!read_group(bundle, dest))
                return false;
        }
        else
        {
            iotrace::trace_file(in.path(), 0, 0, "read metadata");
            auto md = decode_metadata(bundle);

            // If the source is inline, then the data follows the metadata
            if (md->source().style() == types::Source::Style::INLINE)
                read_inline_data(*md);
            if (!dest(move(md)))
                return false;
        }
    }
    return true;
}

template class BaseReader<core::NamedFileDescriptor>;
template class BaseReader<core::AbstractInputFile>;

} // namespace reader

} // namespace arki::metadata
