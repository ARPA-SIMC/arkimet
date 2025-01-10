#ifndef ARKI_SEGMENT_SEQFILE_H
#define ARKI_SEGMENT_SEQFILE_H

#include <arki/defs.h>
#include <arki/core/file.h>
#include <filesystem>
#include <string>

namespace arki {
namespace segment {

struct SequenceFile : public core::File
{
    std::filesystem::path dirname;
    /// Set to true by read_sequence when the sequence has been reset to 0
    /// because the file is new
    bool new_file = false;

    SequenceFile(const std::filesystem::path& dirname);
    ~SequenceFile();

    /**
     * Open the sequence file.
     *
     * This is not automatically done in the constructor, because Writer, when
     * truncating, needs to have the time to recreate its directory before
     * creating the sequence file.
     */
    void open();

    /// Close the sequence file
    void close();

    /// Read the value in the sequence file
    size_t read_sequence();

    /// Write the value to the sequence file
    void write_sequence(size_t val);

    static std::filesystem::path data_fname(size_t pos, DataFormat format);
};

}
}
#endif
