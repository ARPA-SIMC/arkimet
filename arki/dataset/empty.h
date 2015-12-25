#ifndef ARKI_DATASET_EMPTY_H
#define ARKI_DATASET_EMPTY_H

/// Virtual read only dataset that is always empty

#include <arki/dataset/local.h>
#include <string>

namespace arki {
class ConfigFile;
class Metadata;
class Matcher;

namespace dataset {

/**
 * Dataset that is always empty
 */
class Empty : public LocalReader
{
public:
    // Initialise the dataset with the information from the configurationa in 'cfg'
    Empty(const ConfigFile& cfg);
    virtual ~Empty();

    // Nothing to do: the dataset is always empty
    void query_data(const dataset::DataQuery& q, std::function<bool(std::unique_ptr<Metadata>)>) override {}
    void query_summary(const Matcher& matcher, Summary& summary) override {}
    void query_bytes(const dataset::ByteQuery& q, int out) override {}
    size_t produce_nth(metadata_dest_func cons, size_t idx=0) override { return 0; }

    //void rescanFile(const std::string& relpath) override {}
    //size_t repackFile(const std::string& relpath) override { return 0; }
    //size_t removeFile(const std::string& relpath, bool withData=false) override { return 0; }
    //void archiveFile(const std::string& relpath) override {}
    //size_t vacuum() override { return 0; }
};

}
}
#endif
