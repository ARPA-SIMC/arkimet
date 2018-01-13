#ifndef ARKI_DATASET_SEGMENT_SEQFILE_H
#define ARKI_DATASET_SEGMENT_SEQFILE_H

#include <arki/core/file.h>
#include <string>

namespace arki {
namespace dataset {
namespace segment {

struct SequenceFile
{
    std::string dirname;
    core::File fd;

    SequenceFile(const std::string& pathname);
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

    /**
     * Increment the sequence and get the pathname of the file corresponding
     * to the new value, and the corresponding 'offset', that is, the file
     * sequence number itself.
     */
    std::pair<std::string, size_t> next(const std::string& format);

    /**
     * Call next() then open its result with O_EXCL; retry with a higher number
     * if the file already exists.
     *
     * Returns the open file descriptor, and the corresponding 'offset', that
     * is, the file sequence number itself.
     */
    core::File open_next(const std::string& format, size_t& pos);

    void test_add_padding(unsigned size);

    static std::string data_fname(size_t pos, const std::string& format);
};

}
}
}

#endif
