#ifndef ARKI_DATASET_SEGMENT_SEQFILE_H
#define ARKI_DATASET_SEGMENT_SEQFILE_H

#include <arki/core/file.h>
#include <string>

namespace arki {
namespace dataset {
namespace segment {

struct SequenceFile : public core::File
{
    std::string dirname;

    SequenceFile(const std::string& dirname);
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

    static std::string data_fname(size_t pos, const std::string& format);
};

}
}
}

#endif
