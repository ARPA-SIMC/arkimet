#ifndef ARKI_RUNTIME_H
#define ARKI_RUNTIME_H

/// Common code used in most arkimet executables

#include <arki/utils/commandline/parser.h>
#include <arki/runtime/io.h>
#include <arki/runtime/config.h>
#include <arki/runtime/processor.h>
#include <arki/metadata.h>
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

struct CommandLine : public utils::commandline::StandardParserWithManpage
{
    utils::commandline::OptionGroup* infoOpts;
    utils::commandline::OptionGroup* inputOpts;
    utils::commandline::OptionGroup* outputOpts;
    utils::commandline::OptionGroup* dispatchOpts;

    utils::commandline::BoolOption* verbose;
    utils::commandline::BoolOption* debug;
    utils::commandline::BoolOption* status;
    utils::commandline::BoolOption* yaml;
    utils::commandline::BoolOption* json;
    utils::commandline::BoolOption* annotate;
    utils::commandline::BoolOption* dataInline;
    utils::commandline::BoolOption* dataOnly;
    utils::commandline::BoolOption* summary;
    utils::commandline::BoolOption* summary_short;
    utils::commandline::BoolOption* merged;
    utils::commandline::BoolOption* ignore_duplicates;
    utils::commandline::StringOption* restr;
    utils::commandline::StringOption* exprfile;
    utils::commandline::StringOption* qmacro;
    utils::commandline::StringOption* outfile;
    utils::commandline::StringOption* targetfile;
    utils::commandline::StringOption* postprocess;
    utils::commandline::StringOption* report;
    utils::commandline::StringOption* sort;
    utils::commandline::StringOption* files;
    utils::commandline::StringOption* moveok;
    utils::commandline::StringOption* moveko;
    utils::commandline::StringOption* movework;
    utils::commandline::StringOption* summary_restrict;
    utils::commandline::StringOption* validate;
    utils::commandline::VectorOption<utils::commandline::ExistingFile>* postproc_data;
    utils::commandline::VectorOption<utils::commandline::String>* cfgfiles;
    utils::commandline::VectorOption<utils::commandline::String>* dispatch;
    utils::commandline::VectorOption<utils::commandline::String>* testdispatch;

    ConfigFile inputInfo;
    ConfigFile dispatchInfo;
    std::string strquery;
    Matcher query;
    utils::sys::NamedFileDescriptor* output;
    DatasetProcessor* processor;
    MetadataDispatch* dispatcher;
    ProcessorMaker pmaker;

	CommandLine(const std::string& name, int mansection = 1);
	~CommandLine();

	/// Add scan-type options (--files, --moveok, --movework, --moveko)
	void addScanOptions();

	/// Add query-type options (--merged, --file, --cfgfiles)
	void addQueryOptions();

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
     * Open the next data source to process
     *
     * @return the pointer to the datasource, or 0 for no more datasets
     */
    std::unique_ptr<dataset::Reader> openSource(ConfigFile& info);

    /**
     * Process one data source
     *
     * If everything went perfectly well, returns true, else false. It can
     * still throw an exception if things go wrong.
     */
    bool processSource(dataset::Reader& ds, const std::string& name);

    /**
     * Done working with one data source
     *
     * @param successful
     *   true if everything went well, false if there were issues
     * FIXME: put something that contains a status report instead, for
     * FIXME: --status, as well as a boolean for moveok/moveko
     */
    void closeSource(std::unique_ptr<dataset::Reader> ds, bool successful = true);
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
    bool dispatch(std::unique_ptr<Metadata>&& md);
};


}
}
#endif
