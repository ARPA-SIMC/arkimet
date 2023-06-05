#ifndef ARKI_UTILS_COMPRESS_H
#define ARKI_UTILS_COMPRESS_H

#include <arki/core/fwd.h>
#include <arki/utils/sys.h>
#include <arki/core/binary.h>
#include <sys/types.h>
#include <string>
#include <vector>
#include <cstdint>

// zlib forward declaration
struct z_stream_s;
typedef struct z_stream_s z_stream;

namespace arki {
namespace utils {

/**
 * Compression/decompression algorithms.
 *
 * If an algorithm is not implemented, it will return the uncompressed data.
 *
 * It makes sense to always check that the compressed data is actually shorter
 * than the uncompressed one, and if not, keep using the uncompressed data.
 */
namespace compress {

/**
 * Compress with LZO.
 *
 * Fast.
 */
std::vector<uint8_t> lzo(const void* in, size_t in_size);

/**
 * Uncompress LZO compressed data.
 *
 * @param size
 *   The size of the uncompressed data
 */
std::vector<uint8_t> unlzo(const void* in, size_t in_size, size_t out_size);

/**
 * Compressor engine based on Zlib
 */
class ZlibCompressor
{
protected:
	z_stream* strm;

public:
	ZlibCompressor();
	~ZlibCompressor();

    /**
     * Set the data for the encoder/decoder
     */
    void feed_data(const void* buf, size_t len);

	/**
	 * Run an encoder loop filling in the given buffer
	 * 
	 * @returns the count of data written (if the same as len, you need to
	 *          call run() again before feed_data)
	 */
	size_t get(void* buf, size_t len, bool flush = false);
	size_t get(std::vector<uint8_t>& buf, bool flush = false);

	/**
	 * Restart compression after a flush
	 */
	void restart();
};

/**
 * Gunzip the file opened at \a rdfd sending data to the file opened at \a wrfd
 */
void gunzip(int rdfd, const std::string& rdfname, int wrfd, const std::string& wrfname, size_t bufsize=4096);

/**
 * Gunzip the file decompressing it to memory
 */
std::vector<uint8_t> gunzip(const std::string& abspath, size_t bufsize=4096);

/**
 * At constructor time, create an uncompressed version of the given file
 *
 * At destructor time, delete the uncompressed version
 */
struct TempUnzip
{
	std::string fname;

	/**
	 * @param fname
	 *   Refers to the uncompressed file name (i.e. without the trailing .gz)
	 */
	TempUnzip(const std::string& fname);
	~TempUnzip();
};

struct SeekIndex
{
    std::vector<size_t> ofs_unc;
    std::vector<size_t> ofs_comp;

    SeekIndex();

	/// Return the index of the block containing the given uncompressed
	/// offset
	size_t lookup(size_t unc) const;

    /// Read the index from the given file descriptor
    void read(core::File& fd);

	/**
	 * Read the index from the given file
	 *
	 * @returns true if the index was read, false if the index does not
	 * exists, throws an exception in case of other errors
	 */
	bool read(const std::string& fname);
};

struct SeekIndexReader
{
    core::NamedFileDescriptor& fd;
    SeekIndex idx;
    std::vector<uint8_t> last_group;
    size_t last_group_offset = 0;

    SeekIndexReader(core::NamedFileDescriptor& fd);

    std::vector<uint8_t> read(size_t offset, size_t size);
};

struct IndexWriter
{
    /// Number of data items in a compressed block
    size_t groupsize;
    /// Index data to be written
    std::vector<uint8_t> outbuf;
    /// Binary encoder to write to outbuf
    core::BinaryEncoder enc;
    /// Offset of end of last uncompressed data read
    off_t unc_ofs = 0;
    /// Offset of end of last uncompressed block written
    off_t last_unc_ofs = 0;
    /// Offset of end of last compressed data written
    off_t ofs = 0;
    /// Offset of end of last compressed block written
    off_t last_ofs = 0;
    /// Number of data compressed so far
    size_t count = 0;

    IndexWriter(size_t groupsize=512);

    /**
     * Update the index to track the addition of a chunk of data.
     *
     * size is the uncompressed size of the data added
     * gz_size is the compressed size of the data added
     */
    void append(size_t size, size_t gz_size);

    /**
     * Signal the end of a data entry. Returns true if groupsize has been
     * reached and a new compressed block should begin
     */
    bool close_entry();

    /**
     * Mark the end of a data block.
     *
     * gz_size is the size of data that has been written while flushing the
     * compressor
     */
    void close_block(size_t gz_size);

    /**
     * Return true if data has been appended since the last close_block()
     */
    bool has_trailing_data() const;

    /// Return true if this index contains only one group
    bool only_one_group() const;

    /**
     * Write out the index
     */
    void write(core::NamedFileDescriptor& outidx);
};


/**
 * Create a file with a compressed version of the data described by the
 * metadata that it receives.
 *
 * It also creates a compressed file index for faster seeking in the compressed
 * file, that can be optionally written out
 */
class GzipWriter
{
protected:
    core::NamedFileDescriptor& out;
    // Compressor
    ZlibCompressor compressor;
    // Output buffer for the compressor
    std::vector<uint8_t> outbuf;

    /**
     * Write out all data in the compressor buffer, emptying it.
     *
     * Returns the number of bytes written
     */
    size_t flush_compressor();

    // End one compressed block
    void end_block(bool is_final=false);

public:
    IndexWriter idx;

    GzipWriter(core::NamedFileDescriptor& out, size_t groupsize=512);
    ~GzipWriter();

    /**
     * Feed \a buf to the compressor, writing out the data that come out.
     *
     * Returns the number of bytes written
     */
    size_t add(const std::vector<uint8_t>& buf);
    void close_entry();

    void flush();
};

}
}
}

#endif
