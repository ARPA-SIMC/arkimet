#ifndef ARKI_RUNTIME_H
#define ARKI_RUNTIME_H

/// Common code used in most arkimet executables

#include <arki/utils/commandline/parser.h>
#include <arki/runtime/io.h>
#include <arki/runtime/config.h>
#include <arki/runtime/processor.h>
#include <arki/runtime/inputs.h>
#include <arki/metadata.h>
#include <arki/dataset/fwd.h>
#include <arki/dataset/memory.h>
#include <arki/matcher.h>
#include <arki/configfile.h>
#include <string>
#include <memory>
#include <vector>
#include <sys/time.h>

namespace arki {
class Summary;
class Dispatcher;
class Formatter;
class Targetfile;
class Validator;

namespace runtime {
class Source;
class MetadataDispatch;

/**
 * Initialise the libraries that we use and parse the matcher alias database.
 */
void init();

std::unique_ptr<dataset::Reader> make_qmacro_dataset(const ConfigFile& ds_cfg, const ConfigFile& dispatch_cfg, const std::string& qmacroname, const std::string& query, const std::string& url=std::string());

/**
 * Exception raised when the command line parser has handled the current
 * command invocation.
 *
 * For example, this happens when using --validate=list, which prints a list of
 * validators then exits.
 */
struct HandledByCommandLineParser
{
    /// The exit status that we should return from main
    int status;

    HandledByCommandLineParser(int status=0);
    ~HandledByCommandLineParser();
};

struct BaseCommandLine : public utils::commandline::StandardParserWithManpage
{
    utils::commandline::OptionGroup* infoOpts = nullptr;
    utils::commandline::BoolOption* verbose = nullptr;
    utils::commandline::BoolOption* debug = nullptr;

    BaseCommandLine(const std::string& name, int mansection = 1);
};

struct CommandLine : public BaseCommandLine
{
    utils::commandline::OptionGroup* inputOpts = nullptr;
    utils::commandline::OptionGroup* outputOpts = nullptr;
    utils::commandline::OptionGroup* dispatchOpts = nullptr;

    utils::commandline::BoolOption* status = nullptr;
    utils::commandline::BoolOption* yaml = nullptr;
    utils::commandline::BoolOption* json = nullptr;
    utils::commandline::BoolOption* annotate = nullptr;
    utils::commandline::BoolOption* dataInline = nullptr;
    utils::commandline::BoolOption* dataOnly = nullptr;
    utils::commandline::BoolOption* summary = nullptr;
    utils::commandline::BoolOption* summary_short = nullptr;
    utils::commandline::BoolOption* merged = nullptr;
    utils::commandline::BoolOption* ignore_duplicates = nullptr;
    utils::commandline::StringOption* restr = nullptr;
    utils::commandline::StringOption* exprfile = nullptr;
    utils::commandline::StringOption* qmacro = nullptr;
    utils::commandline::StringOption* outfile = nullptr;
    utils::commandline::StringOption* targetfile = nullptr;
    utils::commandline::StringOption* postprocess = nullptr;
    utils::commandline::StringOption* report = nullptr;
    utils::commandline::OptvalStringOption* archive = nullptr;
    utils::commandline::StringOption* sort = nullptr;
    utils::commandline::StringOption* files = nullptr;
    utils::commandline::StringOption* moveok = nullptr;
    utils::commandline::StringOption* moveko = nullptr;
    utils::commandline::StringOption* movework = nullptr;
    utils::commandline::StringOption* copyok = nullptr;
    utils::commandline::StringOption* copyko = nullptr;
    utils::commandline::StringOption* summary_restrict = nullptr;
    utils::commandline::StringOption* validate = nullptr;
    utils::commandline::VectorOption<utils::commandline::ExistingFile>* postproc_data = nullptr;
    utils::commandline::VectorOption<utils::commandline::String>* cfgfiles = nullptr;
    utils::commandline::VectorOption<utils::commandline::String>* dispatch = nullptr;
    utils::commandline::VectorOption<utils::commandline::String>* testdispatch = nullptr;

    Inputs inputs;
    ConfigFile dispatchInfo;
    std::string strquery;
    Matcher query;
    utils::sys::NamedFileDescriptor* output = nullptr;
    DatasetProcessor* processor = nullptr;
    MetadataDispatch* dispatcher = nullptr;
    ProcessorMaker pmaker;

    CommandLine(const std::string& name, int mansection = 1);
    ~CommandLine();

    /// Add scan-type options (--files, --moveok, --movework, --moveko)
    void add_scan_options();

    /// Add query-type options (--merged, --file, --cfgfiles)
    void add_query_options();

	/**
	 * Parse the command line
	 */
	bool parse(int argc, const char* argv[]);

	/**
	 * Set up processing after the command line has been parsed and
	 * additional tweaks have been applied
	 */
	void setupProcessing();

	/**
	 * End processing and flush partial data
	 */
	void doneProcessing();

    /**
     * Instantiate all sources requested on command line.
     *
     * Return true if dest returned true (successful) on all sources.
     */
    bool foreach_source(std::function<bool(Source&)> dest);
};

struct Source
{
    virtual ~Source();
    virtual std::string name() const = 0;
    virtual dataset::Reader& reader() const = 0;
    virtual void open() = 0;
    virtual void close(bool successful) = 0;
    virtual bool process(DatasetProcessor& processor);
    virtual bool dispatch(MetadataDispatch& dispatcher);
};

struct FileSource : public Source
{
    ConfigFile cfg;
    std::shared_ptr<dataset::Reader> m_reader;
    std::string movework;
    std::string moveok;
    std::string moveko;

    FileSource(CommandLine& args, const ConfigFile& info);

    std::string name() const override;
    dataset::Reader& reader() const override;
    void open() override;
    void close(bool successful) override;
};

struct MergedSource : public Source
{
    std::vector<std::shared_ptr<FileSource>> sources;
    std::shared_ptr<dataset::Merged> m_reader;
    std::string m_name;

    MergedSource(CommandLine& args);

    std::string name() const override;
    dataset::Reader& reader() const override;
    void open() override;
    void close(bool successful) override;
};

/// Dispatch metadata
struct MetadataDispatch
{
	const ConfigFile& cfg;
	Dispatcher* dispatcher;
    dataset::Memory results;
	DatasetProcessor& next;
	bool ignore_duplicates;
	bool reportStatus;

	// Used for timings. Read with gettimeofday at the beginning of a task,
	// and summarySoFar will report the elapsed time
	struct timeval startTime;

	// Incremented when a metadata is imported in the destination dataset.
	// Feel free to reset it to 0 anytime.
	int countSuccessful;

	// Incremented when a metadata is imported in the error dataset because it
	// had already been imported.  Feel free to reset it to 0 anytime.
	int countDuplicates;

	// Incremented when a metadata is imported in the error dataset.  Feel free
	// to reset it to 0 anytime.
	int countInErrorDataset;

	// Incremented when a metadata is not imported at all.  Feel free to reset
	// it to 0 anytime.
	int countNotImported;

    /// Directory where we store copyok files
    std::string dir_copyok;

    /// Directory where we store copyko files
    std::string dir_copyko;

    /// File to which we send data that was successfully imported
    std::unique_ptr<core::File> copyok;

    /// File to which we send data that was not successfully imported
    std::unique_ptr<core::File> copyko;


    MetadataDispatch(const ConfigFile& cfg, DatasetProcessor& next, bool test=false);
    ~MetadataDispatch();

    /**
     * Dispatch the data from one source
     *
     * @returns true if all went well, false if any problem happend.
     * It can still throw in case of big trouble.
     */
    bool process(dataset::Reader& ds, const std::string& name);

	// Flush all imports done so far
	void flush();

	// Format a summary of the import statistics so far
	std::string summarySoFar() const;

	// Set startTime to the current time
	void setStartTime();

protected:
    void do_copyok(Metadata& md);
    void do_copyko(Metadata& md);
};


}
}
#endif
