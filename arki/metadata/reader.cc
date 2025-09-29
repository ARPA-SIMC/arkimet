#include "reader.h"
#include "arki/core/binary.h"
#include "arki/core/file.h"
#include "arki/metadata.h"
#include "arki/summary.h"
#include "arki/types/bundle.h"
#include "arki/types/note.h"
#include "arki/types/source.h"
#include "arki/utils/compress.h"
#include "arki/utils/string.h"
#include "arki/utils/yaml.h"
#include "iotrace.h"
#include <sstream>
#include <string>

using namespace std::string_literals;

namespace arki::metadata {

/*
 * BaseReader
 */
BaseReader::BaseReader(const std::filesystem::path& basedir) : basedir(basedir)
{
}

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

/*
 * BaseBinaryReader
 */

template <typename FileType>
BaseBinaryReader<FileType>::BaseBinaryReader(FileType& in_)
    : BaseReader(infer_basedir(in_.path())), in(in_)
{
}

template <typename FileType>
BaseBinaryReader<FileType>::BaseBinaryReader(
    FileType& in, const std::filesystem::path& basedir)
    : BaseReader(basedir), in(in)
{
}

template <typename FileType>
bool BaseBinaryReader<FileType>::read_bundle(types::Bundle& bundle)
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
bool BaseBinaryReader<FileType>::read_group(const types::Bundle& bundle,
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
        auto md = Metadata::read_binary_inner(inner, iver, in.path(), basedir);
        if (!dest(move(md)))
            return false;
    }

    return true;
}

template <typename FileType>
bool BaseBinaryReader<FileType>::read_inline_data(Metadata& md)
{
    if (size_t inline_data_size = md.read_inline_data(in))
        offset += inline_data_size;
    else
        return false;
    return true;
}

template <typename FileType>
std::shared_ptr<Metadata>
BaseBinaryReader<FileType>::decode_metadata(const types::Bundle& bundle) const
{
    core::BinaryDecoder inner(bundle.data);
    return Metadata::read_binary_inner(inner, bundle.version, in.path(),
                                       basedir);
}

template <typename FileType>
void BaseBinaryReader<FileType>::decode_summary(const types::Bundle& bundle,
                                                Summary& summary) const
{
    arki::core::BinaryDecoder inner(bundle.data);
    summary.read_inner(inner, bundle.version, in.path());
}

template <typename FileType>
std::shared_ptr<Metadata> BaseBinaryReader<FileType>::read(bool read_inline)
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
bool BaseBinaryReader<FileType>::read_all(metadata_dest_func dest)
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

template class BaseBinaryReader<core::NamedFileDescriptor>;
template class BaseBinaryReader<core::AbstractInputFile>;

} // namespace reader

/*
 * YamlReader
 */
YamlReader::YamlReader(core::NamedFileDescriptor& in_)
    : BaseReader(reader::infer_basedir(in_.path())), m_path(in_.path()),
      in(core::LineReader::from_fd(in_))
{
}
YamlReader::YamlReader(core::NamedFileDescriptor& in_,
                       const std::filesystem::path& basedir)
    : BaseReader(basedir), m_path(in_.path()),
      in(core::LineReader::from_fd(in_))
{
}
YamlReader::YamlReader(core::AbstractInputFile& in_)
    : BaseReader(reader::infer_basedir(in_.path())), m_path(in_.path()),
      in(core::LineReader::from_abstract(in_))
{
}
YamlReader::YamlReader(core::AbstractInputFile& in_,
                       const std::filesystem::path& basedir)
    : BaseReader(basedir), m_path(in_.path()),
      in(core::LineReader::from_abstract(in_))
{
}

std::shared_ptr<Metadata> YamlReader::read(bool read_inline)
{
    if (in->eof())
        return std::shared_ptr<Metadata>();

    std::shared_ptr<Metadata> res;
    utils::YamlStream yamlStream;
    for (utils::YamlStream::const_iterator i = yamlStream.begin(*in);
         i != yamlStream.end(); ++i)
    {
        if (!res)
            res = std::make_shared<Metadata>();
        types::Code type = types::parseCodeName(i->first);
        std::string val  = utils::str::strip(i->second);
        switch (type)
        {
            case TYPE_NOTE:
                res->add_note(types::Note::decodeString(val));
                break;
            case TYPE_SOURCE:
                res->set_source(
                    types::Source::decodeStringRelative(val, basedir));
                break;
            default: res->set(types::decodeString(type, val));
        }
    }
    return res;
}

bool YamlReader::read_all(metadata_dest_func dest)
{
    while (auto md = read())
    {
        if (!dest(md))
            return false;
    }
    return true;
}

} // namespace arki::metadata
