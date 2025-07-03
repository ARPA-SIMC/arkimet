#ifndef ARKI_RUNTIME_H
#define ARKI_RUNTIME_H

#include <string>
#include <vector>
#include <filesystem>

namespace arki {

/**
 * Hold runtime configuration read from compile-time defaults and environment
 * variables
 */
struct Config
{
    /**
     * Regular string vector with extra convenience methods for file lookup
     */
    struct Dirlist : public std::vector<std::filesystem::path>
    {
        /**
         * Look for the file in all directories.
         *
         * @return the pathname if found, raises an exception if not found
         */
        std::filesystem::path find_file(const std::filesystem::path& fname, bool executable=false) const;

        /**
         * Look for the file in all directories.
         *
         * @return the pathname if found, or the empty path if not found
         */
        std::filesystem::path find_file_noerror(const std::filesystem::path& fname, bool executable=false) const;

        /**
         * List the files if the first directory found
         *
         * The file list is sorted by directory order and then by file name.
         *
         * @param ext
         *   The extension (including the dot) that files must have to be considered
         * @param first_only
         *   If true, limit the list to the first directory found
         */
        std::vector<std::filesystem::path> list_files(const std::string& ext, bool first_only=true) const;

        /**
         * Add the directory from the envirnment variable \a envname (if set)
         * and from the compiled in path CONF_DIR/confdir
         */
        void init_config_and_env(const char* confdir, const char* envname);
    };

    /// Directories where formatter scripts are found
    Dirlist dir_formatter;

    /// Directories where bounding box scripts are found
    Dirlist dir_bbox;

    /// Directories where postprocessor executables are found
    Dirlist dir_postproc;

    /// Directories where query macro scripts are found
    Dirlist dir_qmacro;

    /// Directories where grib scan scripts are found
    Dirlist dir_scan;

    /// Directories where bufr scan scripts are found
    Dirlist dir_scan_bufr;

    /// Directories where odimh5 scan scripts are found
    Dirlist dir_scan_odimh5;

    /// Alias file
    std::filesystem::path file_aliases;

    /// I/O profiling log file
    std::filesystem::path file_iotrace_output;

    /// I/O timeout in milliseconds (0: no timeout, default: 15 minutes)
    unsigned io_timeout_ms = 15 * 1000;

    Config();

    /// Get the runtime configuration
    static Config& get();
};

/**
 * Initialise the libraries that we use and parse the matcher alias database.
 */
void init();

}
#endif
