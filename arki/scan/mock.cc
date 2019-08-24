#include "mock.h"
#include "arki/metadata.h"
#include "arki/metadata/data.h"
#include "arki/segment.h"
#include "arki/core/binary.h"
#include "arki/types/source.h"
#include "arki/utils/sys.h"
#include "arki/utils/sqlite.h"
#include <openssl/evp.h>
#include <cstdlib>

#if OPENSSL_VERSION_NUMBER < 0x10100000L
#include <openssl/engine.h>
#include <cstring>
#endif

using namespace std;
using namespace arki::types;
using namespace arki::utils;


namespace arki {
namespace scan {

MockEngine::MockEngine()
{
    if (const char* mock_db = getenv("ARKI_MOCK_SCAN_DB"))
    {
        db_pathname = mock_db;
        db = new sqlite::SQLiteDB();
        db->open(mock_db);

        by_sha256sum = new sqlite::Query("by_sha256sum", *db);
        by_sha256sum->compile("SELECT md FROM mds WHERE sha256sum=?");
    } else {
        throw std::runtime_error("ARKI_MOCK_SCAN_DB not defined but needed by arkimet mock scanner");
    }

#if OPENSSL_VERSION_NUMBER < 0x10100000L
static bool has_digests = false;
    if (!has_digests)
    {
        OpenSSL_add_all_digests();
        has_digests = true;
    }
#endif
}

MockEngine::~MockEngine()
{
    delete by_sha256sum;
    delete db;
}

namespace {

static const char* hex[] = {
    "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "0a", "0b", "0c", "0d", "0e", "0f",
    "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "1a", "1b", "1c", "1d", "1e", "1f",
    "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "2a", "2b", "2c", "2d", "2e", "2f",
    "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "3a", "3b", "3c", "3d", "3e", "3f",
    "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "4a", "4b", "4c", "4d", "4e", "4f",
    "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "5a", "5b", "5c", "5d", "5e", "5f",
    "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "6a", "6b", "6c", "6d", "6e", "6f",
    "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "7a", "7b", "7c", "7d", "7e", "7f",
    "80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "8a", "8b", "8c", "8d", "8e", "8f",
    "90", "91", "92", "93", "94", "95", "96", "97", "98", "99", "9a", "9b", "9c", "9d", "9e", "9f",
    "a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "aa", "ab", "ac", "ad", "ae", "af",
    "b0", "b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8", "b9", "ba", "bb", "bc", "bd", "be", "bf",
    "c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "ca", "cb", "cc", "cd", "ce", "cf",
    "d0", "d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9", "da", "db", "dc", "dd", "de", "df",
    "e0", "e1", "e2", "e3", "e4", "e5", "e6", "e7", "e8", "e9", "ea", "eb", "ec", "ed", "ee", "ef",
    "f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "fa", "fb", "fc", "fd", "fe", "ff",
};

// From https://wiki.openssl.org/index.php/OpenSSL_1.1.0_Changes
#if OPENSSL_VERSION_NUMBER < 0x10100000L
void *OPENSSL_zalloc(size_t num)
{
    void *ret = OPENSSL_malloc(num);

    if (ret != NULL)
        memset(ret, 0, num);
    return ret;
}

EVP_MD_CTX *EVP_MD_CTX_new(void)
{
    return (EVP_MD_CTX *)OPENSSL_zalloc(sizeof(EVP_MD_CTX));
}

void EVP_MD_CTX_free(EVP_MD_CTX *ctx)
{
    EVP_MD_CTX_cleanup(ctx);
    OPENSSL_free(ctx);
}
#endif

std::string compute_hash(const char* name, const void* data, size_t size)
{
    // See: https://www.openssl.org/docs/man1.1.0/man3/EVP_DigestInit.html
    const EVP_MD *md = EVP_get_digestbyname(name);
    if (!md)
        throw std::invalid_argument(std::string("checksum algorithm not found: ") + name);

    EVP_MD_CTX *mdctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(mdctx, md, NULL);
    EVP_DigestUpdate(mdctx, data, size);

    unsigned char md_value[EVP_MAX_MD_SIZE];
    unsigned int md_len;
    EVP_DigestFinal_ex(mdctx, md_value, &md_len);

    EVP_MD_CTX_free(mdctx);

    std::string res;
    for (unsigned i = 0; i < md_len; ++i)
        res += hex[md_value[i]];
    return res;
}

}

std::shared_ptr<Metadata> MockEngine::by_checksum(const std::string& checksum)
{
    // SELECT file, format, offset, size, md FROM mds WHERE sha256sum=?
    bool found = false;
    by_sha256sum->reset();
    by_sha256sum->bind(1, checksum);
    std::shared_ptr<Metadata> md;
    while (by_sha256sum->step())
    {
        const uint8_t* buf = static_cast<const uint8_t*>(by_sha256sum->fetchBlob(0));
        int len = by_sha256sum->fetchBytes(0);
        core::BinaryDecoder dec((const uint8_t*)buf, len);

        md.reset(new Metadata);
        md->read(dec, db_pathname, false);
        found = true;
    }
    if (!found)
        throw std::invalid_argument("data " + checksum + " not found in mock scan database");
    return md;
}

std::shared_ptr<Metadata> MockEngine::lookup(const uint8_t* data, size_t size)
{
    return by_checksum(compute_hash("SHA256", data, size));
}

std::shared_ptr<Metadata> MockEngine::lookup(const std::vector<uint8_t>& data)
{
    return by_checksum(compute_hash("SHA256", data.data(), data.size()));
}

std::shared_ptr<Metadata> MockEngine::lookup(const std::string& data)
{
    return by_checksum(compute_hash("SHA256", data.data(), data.size()));
}


#if 0
std::shared_ptr<Metadata> MockScanner::scan_data(const std::vector<uint8_t>& data)
{
    std::string checksum = compute_hash("SHA256", data);
    // SELECT file, format, offset, size, md FROM mds WHERE sha256sum=?
    bool found = false;
    by_sha256sum->reset();
    by_sha256sum->bind(1, checksum);
    std::shared_ptr<Metadata> md;
    while (by_sha256sum->step())
    {
        const uint8_t* buf = static_cast<const uint8_t*>(by_fname->fetchBlob(4));
        int len = by_fname->fetchBytes(4);
        BinaryDecoder dec((const uint8_t*)buf, len);

        md.reset(new Metadata);
        md->read(dec, db_pathname, false);

        std::string file = "inbound/" + by_sha256sum->fetchString(0);
        std::string format = by_sha256sum->fetchString(1);
        off_t offset = by_sha256sum->fetch<off_t>(2);
        size_t size = by_sha256sum->fetch<size_t>(3);

        std::vector<uint8_t> data(size);

        sys::File in(file, O_RDONLY);
        if (in.pread(data.data(), data.size(), offset) != (size_t)offset)
            throw std::runtime_error("cannot read all of the metadata from " + file + ":" + std::to_string(offset) + "+" + std::to_string(size));

        md->set_source_inline(
                format,
                metadata::DataManager::get().to_data("grib", std::move(data)));
        found = true;
    }
    if (!found)
        throw std::invalid_argument("data " + checksum + " not found in mock scan database");
    return md;
}

bool MockScanner::scan_segment(std::shared_ptr<segment::Reader> reader, metadata_dest_func dest)
{
    std::string basedir = sys::abspath("inbound");
    std::string file = reader->segment().abspath;
    size_t pos = file.find("inbound/");
    if (pos == std::string::npos)
        throw std::invalid_argument("mock scanner can only scan files in inbound/");
    file = file.substr(pos + 8);

    // SELECT format, offset, size, md FROM mds WHERE file=? ORDER BY offset
    bool found = false;
    by_fname->reset();
    by_fname->bind(1, file);
    while (by_fname->step())
    {
        const uint8_t* buf = static_cast<const uint8_t*>(by_fname->fetchBlob(3));
        int len = by_fname->fetchBytes(3);
        BinaryDecoder dec((const uint8_t*)buf, len);

        std::shared_ptr<Metadata> md(new Metadata);
        md->read(dec, file, false);

        md->set_source(types::Source::createBlob(
            by_fname->fetchString(0),
            basedir,
            file, 
            by_fname->fetch<off_t>(1),
            by_fname->fetch<size_t>(2),
            reader
        ));
        if (!dest(md)) return false;
        found = true;
    }
    if (!found)
        throw std::invalid_argument(file + " not found in mock scan database");
    return true;
}

std::shared_ptr<Metadata> MockScanner::scan_singleton(const std::string& abspath)
{
    std::string data = sys::read_file(abspath);
    std::string checksum = compute_hash("SHA256", data);
    // SELECT file, format, offset, size, md FROM mds WHERE sha256sum=?
    bool found = false;
    by_sha256sum->reset();
    by_sha256sum->bind(1, checksum);
    std::shared_ptr<Metadata> md;
    while (by_sha256sum->step())
    {
        const uint8_t* buf = static_cast<const uint8_t*>(by_fname->fetchBlob(4));
        int len = by_fname->fetchBytes(4);
        BinaryDecoder dec((const uint8_t*)buf, len);

        md.reset(new Metadata);
        md->read(dec, db_pathname, false);
        found = true;
        // no source to be set
    }
    if (!found)
        throw std::invalid_argument("data " + checksum + " for " + abspath + " not found in mock scan database");
    return md;
}

bool MockScanner::scan_pipe(core::NamedFileDescriptor& infd, metadata_dest_func dest)
{
    // This can be left unimplemented: it is only used from python, where
    // proper scanners are available
    throw std::runtime_error("MockScanner::scan_pipe is not implemented");
}
#endif

}
}
