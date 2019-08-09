#ifndef ARKI_REPORT_H
#define ARKI_REPORT_H

/// arki/report - Build a report of an arkimet metadata or summary stream

#include <string>
#include <memory>
#include <iosfwd>
#include <arki/core/fwd.h>
#include <arki/metadata/fwd.h>

namespace arki {
struct Summary;
struct Lua;

class Report
{
protected:
	mutable Lua *L;
	std::string m_filename;
	bool m_accepts_metadata;
	bool m_accepts_summary;

	void create_lua_object();

public:
    Report(const std::string& params = std::string());
    ~Report();

	/// Return true if this report can process metadata
	bool acceptsMetadata() const { return m_accepts_metadata; }

	/// Return true if this report can process summaries
	bool acceptsSummary() const { return m_accepts_summary; }

	/// Load the report described by the given string
	void load(const std::string& params);

	/// Load the report code from the given file
	void loadFile(const std::string& fname);
	/// Load the report code from the given string containing Lua source code
	void loadString(const std::string& buf);

    /// Send Lua's print output to an ostream
    void captureOutput(std::ostream& buf);

    /// Send Lua's print output to an ostream
    void captureOutput(core::AbstractOutputFile& out);

    /// Send Lua's print output to a file descriptor
    void captureOutput(int out);

    /// Process a metadata for the report
    bool eat(std::shared_ptr<Metadata> md);

    /// Process a summary for the report
    bool operator()(Summary& s);

    /// Done sending input: produce the report
    void report();
};

}
#endif
