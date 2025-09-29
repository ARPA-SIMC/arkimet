#ifndef ARKI_METADATA_READER_H
#define ARKI_METADATA_READER_H

#include <arki/core/fwd.h>
#include <arki/metadata/fwd.h>
#include <arki/types/fwd.h>
#include <filesystem>

namespace arki {
class Summary;

namespace metadata {

class BaseReader
{
protected:
    /**
     * Absolute path of the root directory for relative paths in the Metadata's
     * source filenames
     */
    std::filesystem::path basedir;

    // TODO: move offset here from BaseBinaryReader, after adding current
    // offset support to LineReader

public:
    explicit BaseReader(const std::filesystem::path& basedir);

    /// Return the path of the file being read
    virtual std::filesystem::path path() const = 0;

    /**
     * Read a metadata entry.
     *
     * If read_inline is true, in case the associated data is transmitted
     * inline, it is read as well.
     *
     * If read_inline is false, then the associated data, if any, is not read
     * and it is up to the caller to manage its possible presence
     *
     * @returns an empty shared_ptr when end-of-file is reached
     */
    virtual std::shared_ptr<Metadata> read(bool read_inline = true) = 0;

    /**
     * Read all metadata into the given consumer.
     *
     * If the consumer returns false, reading stops after the element that
     * generated the last entry sent to the consumer. Note that this may result
     * in a number of unseen metadata entries, if the file contains a metadata
     * group and the consumer stops partway through a group of metadata.
     *
     * Return true if the consumer accaeted all data, false otherwise.
     */
    virtual bool read_all(metadata_dest_func dest) = 0;
};

namespace reader {

std::filesystem::path infer_basedir(const std::filesystem::path& path);

template <typename FileType> class BaseBinaryReader : public BaseReader
{
protected:
    /// File to read from
    FileType& in;

    /**
     * Offset in the file after the last read operation.
     *
     * This is updated after each read operation.
     *
     * If a read operation raises an exception, the value of offset will be
     * undefined.
     *
     * If a read operation signals end-of-file, offset is left to the end of
     * the last successful read (which may happen in case of trailing null
     * bytes in the file)
     */
    size_t offset = 0;

public:
    /**
     * Create a reader, inferring the basedir from the file path
     */
    explicit BaseBinaryReader(FileType& in);

    /**
     * Create a reader with an explicit basedir
     */
    BaseBinaryReader(FileType& in, const std::filesystem::path& basedir);

    std::filesystem::path path() const override { return in.path(); }
    std::shared_ptr<Metadata> read(bool read_inline = true) override;
    bool read_all(metadata_dest_func dest) override;

    /**
     * Read a medatata bundle.
     *
     * Returns true if the bundle was reader, false on EOF.
     */
    bool read_bundle(types::Bundle& bundle);

    /**
     * Read a metadata group into the given consumer
     */
    bool read_group(const types::Bundle& bundle, metadata_dest_func dest);

    /**
     * Read the inline data associated with the given metadata
     */
    bool read_inline_data(Metadata& md);

    /// Decode a Metadata from a type bundle
    std::shared_ptr<Metadata>
    decode_metadata(const types::Bundle& bundle) const;

    /// Decode a Summary from a type bundle
    void decode_summary(const types::Bundle& bundle, Summary& summary) const;
};

} // namespace reader

/**
 * Read metadata from a file
 */
class BinaryReader : public reader::BaseBinaryReader<core::NamedFileDescriptor>
{
public:
    using reader::BaseBinaryReader<core::NamedFileDescriptor>::BaseBinaryReader;
};

/**
 * Read metadata from an abstract file
 */
class AbstractFileBinaryReader
    : public reader::BaseBinaryReader<core::AbstractInputFile>
{
public:
    using reader::BaseBinaryReader<core::AbstractInputFile>::BaseBinaryReader;
};

/**
 * Read metadata from a YAML stream
 */
class YamlReader : public BaseReader
{
protected:
    // TODO: add a path() method to LineReader
    std::filesystem::path m_path;
    std::shared_ptr<core::LineReader> in;

public:
    /// Create a reader, inferring the basedir from the file path
    explicit YamlReader(core::NamedFileDescriptor& in);
    /// Create a reader, inferring the basedir from the file path
    explicit YamlReader(core::AbstractInputFile& in);
    /// Create a reader with an explicit basedir
    YamlReader(core::NamedFileDescriptor& in,
               const std::filesystem::path& basedir);
    /// Create a reader with an explicit basedir
    YamlReader(core::AbstractInputFile& in,
               const std::filesystem::path& basedir);

    std::filesystem::path path() const override { return m_path; }
    std::shared_ptr<Metadata> read(bool read_inline = true) override;
    bool read_all(metadata_dest_func dest) override;
};

} // namespace metadata
} // namespace arki

#endif
