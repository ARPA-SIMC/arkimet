#ifndef ARKI_RUNTIME_H
#define ARKI_RUNTIME_H

#include <string>
#include <vector>

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
    struct Dirlist : public std::vector<std::string>
    {
        /**
         * Look for the file in all directories.
         *
         * @return the pathname if found, raises an exception if not found
         */
        std::string find_file(const std::string& fname, bool executable=false) const;

        /**
         * Look for the file in all directories.
         *
         * @return the pathname if found, or the empty string if not found
         */
        std::string find_file_noerror(const std::string& fname, bool executable=false) const;

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
        std::vector<std::string> list_files(const std::string& ext, bool first_only=true) const;

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
    std::string file_aliases;

    /// I/O profiling log file
    std::string file_iotrace_output;

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
