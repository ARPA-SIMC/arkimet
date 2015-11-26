#include "config.h"
#include <arki/utils/compress.h>
#include <arki/utils/fd.h>
#include <arki/utils/string.h>
#include <arki/utils/sys.h>
#include <arki/utils/gzip.h>
#include <arki/metadata.h>
#include <algorithm>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <utime.h>
#include <arpa/inet.h>
#include <errno.h>
#include <zlib.h>

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

void ZlibCompressor::feedData(const void* buf, size_t len)
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

TempUnzip::TempUnzip(const std::string& fname)
	: fname(fname)
{
	// zcat gzfname > fname
	string gzfname = fname + ".gz";
	int rdfd = open(gzfname.c_str(), O_RDONLY);
	utils::fd::HandleWatch hwrd(gzfname, rdfd);

	int wrfd = open(fname.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0666);
	utils::fd::HandleWatch hwwr(fname, wrfd);

	gunzip(rdfd, gzfname, wrfd, fname);

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
    size_t idxcount = st.st_size / sizeof(uint32_t) / 2;
    vector<uint32_t> diskidx(idxcount * 2);
    fd.read_all_or_throw(diskidx.data(), diskidx.size() * sizeof(uint32_t));
	ofs_unc.reserve(idxcount + 1);
	ofs_comp.reserve(idxcount + 1);
	ofs_unc.push_back(0);
	ofs_comp.push_back(0);
	for (size_t i = 0; i < idxcount; ++i)
	{
		ofs_unc.push_back(ofs_unc[i] + ntohl(diskidx[i * 2]));
		ofs_comp.push_back(ofs_comp[i] + ntohl(diskidx[i * 2 + 1]));
	}
}

off_t filesize(const std::string& file)
{
    // First try the uncompressed version
    std::unique_ptr<struct stat> st = sys::stat(file);
    if (st.get() != NULL)
        return st->st_size;

    // Then try the gzipped version
    st = sys::stat(file + ".gz");
    if (st.get() != NULL)
    {
		// Try to get the uncompressed size via the index
		SeekIndex idx;
		if (idx.read(file + ".gz.idx"))
		{
			// Try to get the uncompressed size via the index
			return idx.ofs_unc.back();
        } else {
            // Seek through the whole file (ouch, slow)
            // for now, just throw an exception
            stringstream ss;
            ss << "cannot compute file size of " << file << ".gz: compressed file has no index; to compute its uncompressed size it needs to be fully uncompressed. Please do it by hand and then recompress generating its .gz.idx index";
            throw std::runtime_error(ss.str());
        }
    }

	// If everything fails, return 0
	return 0;
}


DataCompressor::DataCompressor(const std::string& outfile, size_t groupsize)
    : groupsize(groupsize),
      outfd(outfile + ".gz", O_WRONLY | O_CREAT | O_EXCL, 0666),
      outidx(outfile + ".gz.idx", O_WRONLY | O_CREAT | O_EXCL, 0666),
      outbuf(4096*2), unc_ofs(0), last_unc_ofs(0), last_ofs(0), count(0)
{
}

DataCompressor::~DataCompressor()
{
	flush();
}

bool DataCompressor::eat(unique_ptr<Metadata>&& md)
{
    add(md->getData());
    return true;
}

void DataCompressor::add(const std::vector<uint8_t>& buf)
{
    // Compress data
    compressor.feedData(buf.data(), buf.size());
    while (true)
    {
        size_t len = compressor.get(outbuf, false);
        if (len > 0)
            outfd.write_all_or_throw(outbuf.data(), len);
        if (len < outbuf.size())
            break;
    }

    unc_ofs += buf.size();

	if (count > 0 && (count % groupsize) == 0)
		endBlock();

	++count;
}

void DataCompressor::endBlock(bool is_final)
{
    while (true)
    {
        size_t len = compressor.get(outbuf, true);
        if (len > 0)
            outfd.write_all_or_throw(outbuf.data(), len);
        if (len < outbuf.size())
            break;
    }

    // Write last block size to the index
    off_t cur = lseek(outfd, 0, SEEK_CUR);
    uint32_t ofs = htonl(unc_ofs - last_unc_ofs);
    uint32_t last_size = htonl(cur - last_ofs);
    outidx.write_all_or_throw(&ofs, sizeof(ofs));
    outidx.write_all_or_throw(&last_size, sizeof(last_size));
    last_ofs = cur;
    last_unc_ofs = unc_ofs;

    if (!is_final) compressor.restart();
}

void DataCompressor::flush()
{
	if (outfd != -1 && outidx != -1)
		if (unc_ofs > 0 && last_unc_ofs != unc_ofs)
			endBlock(true);

    outfd.close();
    outidx.close();
}

}
}
}
