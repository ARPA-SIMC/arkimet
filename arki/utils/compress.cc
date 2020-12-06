#include "config.h"
#include "arki/utils/compress.h"
#include "arki/utils/sys.h"
#include "arki/utils/gzip.h"
#include "arki/utils/accounting.h"
#include <algorithm>
#include <fcntl.h>
#include <utime.h>
#include <zlib.h>
#include <sstream>

#ifdef HAVE_LZO
#include "lzo/lzoconf.h"
#include "lzo/lzo1x.h"
#elif HAVE_MINILZO
#include "minilzo.h"
#define HAVE_LZO
#endif

using namespace std;
using namespace arki::utils;

namespace arki {
namespace utils {
namespace compress {

static void require_lzo_init()
{
    static bool done = false;
    if (!done)
    {
        if (lzo_init() != LZO_E_OK)
            throw std::runtime_error("cannot initialize LZO library: lzo_init() failed (this usually indicates a compiler bug)");
        done = true;
    }
}


std::vector<uint8_t> lzo(const void* in, size_t in_size)
{
#ifdef HAVE_LZO
    require_lzo_init();

    // LZO work memory
    std::vector<uint8_t> wrkmem(LZO1X_1_MEM_COMPRESS);

    // Output buffer
    std::vector<uint8_t> out(in_size + in_size / 16 + 64 + 3);
    lzo_uint out_len = out.size();

	// Compress
	int r = lzo1x_1_compress(
			(lzo_bytep)in, in_size,
			(lzo_bytep)out.data(), &out_len,
			(lzo_bytep)wrkmem.data());
    if (r != LZO_E_OK)
    {
        stringstream ss;
        ss << "cannot compress data with LZO: LZO internal error " << r;
        throw std::runtime_error(ss.str());
    }

    // If the size did not decrease, return the uncompressed data
    if (out_len >= in_size)
        return std::vector<uint8_t>((const uint8_t*)in, (const uint8_t*)in + in_size);

    // Resize output to match the compressed length
    out.resize(out_len);

    return out;
#else
    return std::vector<uint8_t>((const uint8_t*)in, (const uint8_t*)in + in_size);
#endif
}

std::vector<uint8_t> unlzo(const void* in, size_t in_size, size_t out_size)
{
#ifdef HAVE_LZO
    require_lzo_init();

    std::vector<uint8_t> out(out_size);
    lzo_uint new_len = out_size;
    int r = lzo1x_decompress_safe((lzo_bytep)in, in_size, (lzo_bytep)out.data(), &new_len, NULL);
    if (r != LZO_E_OK || new_len != out_size)
    {
        stringstream ss;
        ss << "cannot decompress data with LZO: internal error " << r;
        throw std::runtime_error(ss.str());
    }

    return out;
#else
    throw std::runtime_error("cannot decompress data with LZO: LZO support was not available at compile time");
#endif
}

ZlibCompressor::ZlibCompressor() : strm(0)
{
	/* allocate deflate state */
	strm = new z_stream;
	strm->zalloc = Z_NULL;
	strm->zfree = Z_NULL;
	strm->opaque = Z_NULL;
    int ret = deflateInit2(strm, 9, Z_DEFLATED, 15 + 16, 9, Z_DEFAULT_STRATEGY);
    if (ret != Z_OK)
        throw std::runtime_error("zlib initialization failed");
}

ZlibCompressor::~ZlibCompressor()
{
	if (strm)
	{
		(void)deflateEnd(strm);
		delete strm;
	}
}

void ZlibCompressor::feed_data(const void* buf, size_t len)
{
    strm->avail_in = len;
    strm->next_in = (Bytef*)buf;
}

size_t ZlibCompressor::get(void* buf, size_t len, bool flush)
{
	int z_flush = flush ? Z_FINISH : Z_NO_FLUSH;
	strm->avail_out = len;
	strm->next_out = (Bytef*)buf;
	int ret = deflate(strm, z_flush);    /* no bad return value */
    if (ret == Z_STREAM_ERROR)
        throw std::runtime_error("zlib deflate failed");
    return len - strm->avail_out;
}

size_t ZlibCompressor::get(std::vector<uint8_t>& buf, bool flush)
{
    return get(buf.data(), buf.size(), flush);
}

void ZlibCompressor::restart()
{
    if (deflateReset(strm) != Z_OK)
        throw runtime_error("zlib deflate stream reset error");
}

void gunzip(int rdfd, const std::string& rdfname, int wrfd, const std::string& wrfname, size_t bufsize)
{
    // (Re)open the compressed file
    int rdfd1 = dup(rdfd);
    gzip::File gzfd(rdfname, rdfd1, "rb");
    sys::NamedFileDescriptor out(wrfd, wrfname);
    vector<char> buf(bufsize);
    while (true)
    {
        unsigned count = gzfd.read(buf.data(), buf.size());
        out.write_all_or_throw(buf.data(), count);
        if (count < bufsize)
            break;
    }
    // Let the caller close file rdfd and wrfd
}

std::vector<uint8_t> gunzip(const std::string& abspath, size_t bufsize)
{
    gzip::File gzfd(abspath, "rb");
    vector<uint8_t> buf(bufsize);
    vector<uint8_t> res;
    while (true)
    {
        unsigned count = gzfd.read(buf.data(), buf.size());
        res.insert(res.end(), buf.begin(), buf.begin() + count);
        if (count < bufsize)
            break;
    }
    return res;
}

TempUnzip::TempUnzip(const std::string& fname)
    : fname(fname)
{
    // zcat gzfname > fname
    string gzfname = fname + ".gz";
    sys::File rdfd(gzfname, O_RDONLY);

    sys::File wrfd(fname, O_WRONLY | O_CREAT | O_EXCL, 0666);

    gunzip(rdfd, gzfname, wrfd, fname);
    rdfd.close();
    wrfd.close();

    // Set the same timestamp as the compressed file
    std::unique_ptr<struct stat> st = sys::stat(gzfname);
    struct utimbuf times;
    times.actime = st->st_atime;
    times.modtime = st->st_mtime;
    utime(fname.c_str(), &times);
}

TempUnzip::~TempUnzip()
{
	::unlink(fname.c_str());
}



SeekIndex::SeekIndex()
{
    ofs_unc.push_back(0);
    ofs_comp.push_back(0);
}

size_t SeekIndex::lookup(size_t unc) const
{
	vector<size_t>::const_iterator i = upper_bound(ofs_unc.begin(), ofs_unc.end(), unc);
	return i - ofs_unc.begin() - 1;
}

bool SeekIndex::read(const std::string& fname)
{
    sys::File fd(fname);
    if (!fd.open_ifexists(O_RDONLY)) return false;
    read(fd);
    return true;
}

void SeekIndex::read(sys::File& fd)
{
    struct stat st;
    fd.fstat(st);
    size_t idxcount = st.st_size / 8 / 2;
    vector<uint8_t> diskidx(st.st_size);
    fd.read_all_or_throw(diskidx.data(), diskidx.size());
    ofs_unc.reserve(idxcount + 1);
    ofs_comp.reserve(idxcount + 1);
    core::BinaryDecoder dec(diskidx);
    for (size_t i = 0; i < idxcount; ++i)
    {
        ofs_unc.push_back(ofs_unc[i] + dec.pop_uint(8, "uncompressed index"));
        ofs_comp.push_back(ofs_comp[i] + dec.pop_uint(8, "compressed index"));
    }
}


SeekIndexReader::SeekIndexReader(core::NamedFileDescriptor& fd)
    : fd(fd)
{
}

std::vector<uint8_t> SeekIndexReader::read(size_t offset, size_t size)
{
    if (offset < last_group_offset || offset + size > last_group_offset + last_group.size())
    {
        size_t block = idx.lookup(offset);
        if (block >= idx.ofs_comp.size())
            throw std::runtime_error("requested read of offset past the end of gzip file");

        size_t gz_offset = idx.ofs_comp[block];
        fd.lseek(gz_offset, SEEK_SET);

        // Open the compressed chunk
        int fd1 = fd.dup();
        utils::gzip::File gzfd(fd.name(), fd1, "rb");
        last_group_offset = idx.ofs_unc[block];
        acct::gzip_idx_reposition_count.incr();

        if (block + 1 >= idx.ofs_comp.size())
        {
            // Decompress until the end of the file
            last_group = gzfd.read_all();
        } else {
            // Read and uncompress the compressed chunk
            size_t unc_size = idx.ofs_unc[block + 1] - idx.ofs_unc[block];
            last_group.resize(unc_size);
            gzfd.read_all_or_throw(last_group.data(), last_group.size());
        }
    }
    size_t group_offset = offset - last_group_offset;
    if (group_offset + size > last_group.size())
        throw std::runtime_error("requested read of offset past the end of gzip file");
    return std::vector<uint8_t>(last_group.begin() + group_offset, last_group.begin() + group_offset + size);
}


IndexWriter::IndexWriter(size_t groupsize)
    : groupsize(groupsize), enc(outbuf)
{
}

void IndexWriter::append(size_t size, size_t gz_size)
{
    ofs += gz_size;
    unc_ofs += size;
}

bool IndexWriter::close_entry()
{
    ++count;
    if (!groupsize) return false;
    return (count % groupsize) == 0;
}

void IndexWriter::close_block(size_t gz_size)
{
    ofs += gz_size;

    // Write last block size to the index
    size_t unc_size = unc_ofs - last_unc_ofs;
    size_t size = ofs - last_ofs;
    enc.add_unsigned(unc_size, 8);
    enc.add_unsigned(size, 8);
    last_ofs = ofs;
    last_unc_ofs = unc_ofs;
}

bool IndexWriter::has_trailing_data() const
{
    return unc_ofs > 0 && last_unc_ofs != unc_ofs;
}

bool IndexWriter::only_one_group() const
{
    return outbuf.empty() || (!has_trailing_data() && outbuf.size() == 16);
}

void IndexWriter::write(core::NamedFileDescriptor& outidx)
{
    outidx.write_all_or_throw(outbuf.data(), outbuf.size());
}


GzipWriter::GzipWriter(core::NamedFileDescriptor& out, size_t groupsize)
    : out(out), outbuf(4096 * 2), idx(groupsize)
{
}

GzipWriter::~GzipWriter()
{
}

size_t GzipWriter::add(const std::vector<uint8_t>& buf)
{
    size_t written = 0;
    compressor.feed_data(buf.data(), buf.size());
    while (true)
    {
        size_t len = compressor.get(outbuf, false);
        if (len > 0)
        {
            out.write_all_or_throw(outbuf.data(), len);
            written += len;
        }
        if (len < outbuf.size())
            break;
    }

    idx.append(buf.size(), written);
    return written;
}

size_t GzipWriter::flush_compressor()
{
    size_t written = 0;
    while (true)
    {
        // Flush the compressor
        size_t len = compressor.get(outbuf, true);
        if (len > 0)
        {
            out.write_all_or_throw(outbuf.data(), len);
            written += len;
        }
        if (len < outbuf.size())
            break;
    }
    return written;
}

void GzipWriter::flush()
{
    if (idx.has_trailing_data())
        end_block(true);
}

void GzipWriter::close_entry()
{
    if (idx.close_entry())
        end_block();
}

void GzipWriter::end_block(bool is_final)
{
    size_t gz_size = GzipWriter::flush_compressor();
    idx.close_block(gz_size);
    if (!is_final) compressor.restart();
}

}
}
}
