#ifndef ARKI_DATASET_TARGETFILE_H
#define ARKI_DATASET_TARGETFILE_H

/// Compute names for segments in an arkimet dataset

#include <string>
#include <vector>

namespace arki {

class Metadata;
class ConfigFile;

namespace matcher {
class OR;
}

namespace dataset {

/**
 * Generator for filenames the dataset in which to store a metadata
 */
struct TargetFile
{
	virtual ~TargetFile() {}
	virtual std::string operator()(const Metadata& md) = 0;

    /**
     * Check if a given path (even a partial path) can contain things that
     * match the given matcher.
     *
     * Currently it can only look at the reftime part of the matcher.
     */
    virtual bool pathMatches(const std::string& path, const matcher::OR& m) const = 0;

	/**
	 * Create a TargetFile according to the given configuration.
	 *
	 * Usually, only the 'step' configuration key is considered.
	 */
	static TargetFile* create(const ConfigFile& cfg);

	/**
	 * Return the list of available steps
	 */
	static std::vector<std::string> stepList();
};

}
}
#endif
