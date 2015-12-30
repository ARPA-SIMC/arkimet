#ifndef ARKI_DATASET_DATA_DIR_H
#define ARKI_DATASET_DATA_DIR_H

/// Directory based data collection

#include <arki/defs.h>
#include <arki/dataset/segment.h>

namespace arki {
class Metadata;

namespace dataset {
namespace segment {
namespace dir {

struct SequenceFile
{
    std::string dirname;
    std::string pathname;
    int fd;

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
    void open_next(const std::string& format, std::string& absname, size_t& pos, int& fd);

    static std::string data_fname(size_t pos, const std::string& format);
};

class Segment : public dataset::Segment
{
public:
    std::string format;
    SequenceFile seqfile;

protected:
    /**
     * Write the data in md to a file. Delete the file in case of errors. Close fd
     * in any case. Return the size of the data that has been written.
     */
    virtual size_t write_file(Metadata& md, int fd, const std::string& absname);

public:
    Segment(const std::string& format, const std::string& relname, const std::string& absname);
    ~Segment();

    void open();
    void close();

    off_t append(Metadata& md) override;
    off_t append(const std::vector<uint8_t>& buf) override;
    Pending append(Metadata& md, off_t* ofs) override;

    /**
     * Append a hardlink to the data pointed by md.
     *
     * This of course only works if it is possible to hardlink from this
     * segment to the file pointed by md. That is, if they are in the same file
     * system.
     *
     * @returns the offset in the segment at which md was appended
     */
    off_t link(const std::string& absname);

    State check(const metadata::Collection& mds, bool quick=true) override;
    size_t remove() override;
    void truncate(size_t offset) override;
    Pending repack(const std::string& rootdir, metadata::Collection& mds) override;

    /// Call f for each nnnnnn.format file in the directory segment, passing the file name
    void foreach_datafile(std::function<void(const char*)> f);

protected:
    virtual std::unique_ptr<dir::Segment> make_segment(const std::string& format, const std::string& relname, const std::string& absname);
};

/**
 * Same as Writer, but uses ftruncate to write dummy empty file with the
 * original size but that take up no blocks in the file system. This is used
 * only for testing.
 */
class HoleSegment : public Segment
{
protected:
    size_t write_file(Metadata& md, int fd, const std::string& absname) override;

public:
    HoleSegment(const std::string& format, const std::string& relname, const std::string& absname);

    State check(const metadata::Collection& mds, bool quick=true) override;

protected:
    std::unique_ptr<dir::Segment> make_segment(const std::string& format, const std::string& relname, const std::string& absname) override;
};

class OstreamWriter : public segment::OstreamWriter
{
protected:
    sigset_t blocked;

public:
    OstreamWriter();
    virtual ~OstreamWriter();

    size_t stream(Metadata& md, std::ostream& out) const override;
    size_t stream(Metadata& md, int out) const override;
};


}
}
}
}

#endif
