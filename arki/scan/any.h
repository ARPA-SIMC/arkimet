#ifndef ARKI_SCAN_ANY_H
#define ARKI_SCAN_ANY_H

/// Format-independent metadata extraction and validation

#include <arki/libconfig.h>
#include <arki/defs.h>
#include <arki/core/fwd.h>
#include <arki/types/fwd.h>
#include <vector>
#include <string>
#include <ctime>
#include <sys/types.h>

namespace arki {
class Metadata;

namespace scan {

/**
 * Scan the given file, sending its metadata to a consumer.
 *
 * If `filename`.metadata exists and its timestamp is the same of the file or
 * newer, it will be used instead of the file.
 */
bool scan(const std::string& file, std::shared_ptr<core::Lock> lock, metadata_dest_func dest);

/**
 * Scan the given file without format autodetection, sending its metadata to a
 * consumer.
 *
 * If `filename`.metadata exists and its timestamp is the same of the file or
 * newer, it will be used instead of the file.
 *
 * @return true if the file has been scanned, false if the file is in a format
 * that is not supported or recognised.
 */
bool scan(const std::string& file, std::shared_ptr<core::Lock> lock, const std::string& format, metadata_dest_func dest);

/**
 * Return true if the file exists, either uncompressed or compressed
 */
bool exists(const std::string& file);

/**
 * Return true if the file exists, and is compressed
 */
bool isCompressed(const std::string& file);

/**
 * Return the file mtime, whether it is compressed or not
 */
time_t timestamp(const std::string& file);

/**
 * Reconstruct raw data based on a metadata and a value
 */
std::vector<uint8_t> reconstruct(const std::string& format, const Metadata& md, const std::string& value);

/**
 * Return the update sequence number for this data
 *
 * The data associated to the metadata is read and scanned if needed.
 *
 * @retval
 *   The update sequence number found. This is left untouched if the function
 *   returns false.
 * @returns
 *   true if the update sequence number could be found, else false
 *
 */
bool update_sequence_number(Metadata& md, int& usn);

/**
 * Return the update sequence number for this data
 *
 * @retval
 *   The update sequence number found. This is left untouched if the function
 *   returns false.
 * @returns
 *   true if the update sequence number could be found, else false
 *
 */
bool update_sequence_number(const types::source::Blob& source, int& usn);

}
}
#endif
