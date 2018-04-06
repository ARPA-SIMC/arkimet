#ifndef ARKI_UTILS_COMPRESS_H
#define ARKI_UTILS_COMPRESS_H

/// Compression/decompression utilities

#include <arki/libconfig.h>
#include <arki/core/fwd.h>
#include <arki/utils/sys.h>
#include <sys/types.h>
#include <string>
#include <vector>

// zlib forward declaration
struct z_stream_s;
typedef struct z_stream_s z_stream;

namespace arki {
struct Metadata;

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
void gunzip(int rdfd, const std::string& rdfname, int wrfd, const std::string& wrfname, size_t bufsize = 4096);

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

/**
 * Return the uncompressed size of a file
 *
 * @param fnam
 *   Refers to the uncompressed file name (i.e. without the trailing .gz)
 */
off_t filesize(const std::string& file);

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

public:
    GzipWriter(core::NamedFileDescriptor& out);
    ~GzipWriter();

    /**
     * Feed \a buf to the compressor, writing out the data that come out.
     *
     * Returns the number of bytes written
     */
    size_t add(const std::vector<uint8_t>& buf);

    void flush();
};

/**
 * Create a file with a compressed version of the data described by the
 * metadata that it receives.
 *
 * It also creates a compressed file index for faster seeking in the compressed
 * file.
 */
class GzipIndexingWriter : public GzipWriter
{
protected:
    /// Number of data items in a compressed block
    size_t groupsize;
    /// Index output
    core::NamedFileDescriptor& outidx;
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

    // End one compressed block
    void end_block(bool is_final=false);

public:
    GzipIndexingWriter(core::NamedFileDescriptor& out, core::NamedFileDescriptor& outidx, size_t groupsize=512);
    ~GzipIndexingWriter();

    void add(const std::vector<uint8_t>& buf);

    void flush();
};


class DataCompressor
{
protected:
	// Number of data items in a compressed block
	size_t groupsize;
    // Compressed output
    core::File outfd;
    // Index output
    core::File outidx;
	// Compressor
	ZlibCompressor compressor;
	// Output buffer for the compressor
    std::vector<uint8_t> outbuf;
	// Offset of end of last uncompressed data read
	off_t unc_ofs;
	// Offset of end of last uncompressed block written
	off_t last_unc_ofs;
	// Offset of end of last compressed data written
	off_t last_ofs;
	// Number of data compressed so far
	size_t count;

    // End one compressed block
    void endBlock(bool is_final=false);

public:
    DataCompressor(const std::string& outfile, size_t groupsize = 512);
    ~DataCompressor();

    bool eat(std::unique_ptr<Metadata>&& md);

    void add(const std::vector<uint8_t>& buf);

    void flush();
};

}
}
}

// vim:set ts=4 sw=4:
#endif
