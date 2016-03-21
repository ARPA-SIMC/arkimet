#ifndef ARKI_METADATA_XARGS_H
#define ARKI_METADATA_XARGS_H

/// Cluster a metadata stream and run a progrgam on each batch

#include <arki/metadata/clusterer.h>

namespace arki {
namespace metadata {

namespace xargs {

struct Tempfile
{
    std::string pathname_template;
    char* pathname;
    int fd;

    Tempfile();
    ~Tempfile();

    void set_template(const std::string& tpl);
    void open();
    void close();
    void close_nothrow();
    bool is_open() const;
};

}

class Xargs : public Clusterer
{
protected:
    xargs::Tempfile tempfile;

    void start_batch(const std::string& new_format) override;
    void add_to_batch(Metadata& md, const std::vector<uint8_t>& buf) override;
    void flush_batch() override;

    int run_child();

public:
    /**
     * Command to run for each batch, split in components, the first of which
     * is the executable name.
     *
     * For each invocation, command will be appended the name of the temporary
     * file with the batch data.
     */
    std::vector<std::string> command;

    /**
     * Index of the filename argument in the command.
     *
     * If it is a valid index for command, replace that element with the file
     * name before running the command on a batch.
     *
     * If it is -1, append the file name (default).
     *
     * Else, do not pass the file name on the command line.
     */
    int filename_argument;

    Xargs();
    ~Xargs()
    {
    }

    void set_max_bytes(const std::string& val);
    void set_interval(const std::string& val);
};

}
}

#endif
