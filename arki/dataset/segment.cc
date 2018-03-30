#include "segment.h"
#include "segment/concat.h"
#include "segment/lines.h"
#include "segment/dir.h"
#include "segment/tar.h"
#include "arki/configfile.h"
#include "arki/exceptions.h"
#include "arki/scan/any.h"
#include "arki/metadata/collection.h"
#include "arki/metadata.h"
#include "arki/types/source/blob.h"
#include "arki/utils.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"

using namespace std;
using namespace arki::utils;

namespace arki {
namespace dataset {

Segment::Segment(const std::string& root, const std::string& relname, const std::string& absname)
    : root(root), relname(relname), absname(absname)
{
}

Segment::~Segment()
{
}


namespace segment {

Writer::PendingMetadata::PendingMetadata(Metadata& md, std::unique_ptr<types::source::Blob> new_source)
    : md(md), new_source(new_source.release())
{
}

Writer::PendingMetadata::PendingMetadata(PendingMetadata&& o)
    : md(o.md), new_source(o.new_source)
{
    o.new_source = nullptr;
}

Writer::PendingMetadata::~PendingMetadata()
{
    delete new_source;
}

void Writer::PendingMetadata::set_source()
{
    std::unique_ptr<types::source::Blob> source(new_source);
    new_source = 0;
    md.set_source(move(source));
}




void Checker::test_truncate(const metadata::Collection& mds, unsigned data_idx)
{
    const auto& s = mds[data_idx].sourceBlob();
    test_truncate(s.offset);
}


std::string State::to_string() const
{
    vector<const char*> res;
    if (value == SEGMENT_OK)         res.push_back("OK");
    if (value & SEGMENT_DIRTY)       res.push_back("DIRTY");
    if (value & SEGMENT_UNALIGNED)   res.push_back("UNALIGNED");
    if (value & SEGMENT_MISSING)     res.push_back("MISSING");
    if (value & SEGMENT_DELETED)     res.push_back("DELETED");
    if (value & SEGMENT_CORRUPTED)   res.push_back("CORRUPTED");
    if (value & SEGMENT_ARCHIVE_AGE) res.push_back("ARCHIVE_AGE");
    if (value & SEGMENT_DELETE_AGE)  res.push_back("DELETE_AGE");
    return str::join(",", res.begin(), res.end());
}

std::ostream& operator<<(std::ostream& o, const State& s)
{
    return o << s.to_string();
}

namespace {

struct BaseManager : public segment::Manager
{
    bool mockdata;

    BaseManager(const std::string& root, bool mockdata=false) : Manager(root), mockdata(mockdata) {}

    // Instantiate the right Segment implementation for a segment that already
    // exists. Returns 0 if the segment does not exist.
    std::shared_ptr<Writer> create_writer_for_existing_segment(const std::string& format, const std::string& relname, const std::string& absname, bool nullptr_on_error=false)
    {
        std::unique_ptr<struct stat> st = sys::stat(absname);
        std::shared_ptr<Writer> res;
        if (!st.get())
            st = sys::stat(absname + ".gz");
        if (!st.get())
        {
            st = sys::stat(absname + ".tar");
            if (st.get())
                throw_consistency_error(
                        "getting writer for " + format + " file " + relname,
                        "cannot write to .tar segments");
        }
        if (!st.get())
            return res;

        if (S_ISDIR(st->st_mode))
        {
            if (dir::can_store(format))
            {
                if (mockdata)
                    res.reset(new dir::HoleWriter(format, root, relname, absname));
                else
                    res.reset(new dir::Writer(format, root, relname, absname));
            } else {
                if (nullptr_on_error)
                    return res;
                throw_consistency_error(
                        "getting writer for " + format + " file " + relname,
                        "format not supported");
            }
        } else {
            if (format == "grib" || format == "grib1" || format == "grib2")
            {
                if (mockdata)
                    res.reset(new concat::HoleWriter(root, relname, absname));
                else
                    res.reset(new concat::Writer(root, relname, absname));
            } else if (format == "bufr") {
                if (mockdata)
                    res.reset(new concat::HoleWriter(root, relname, absname));
                else
                    res.reset(new concat::Writer(root, relname, absname));
            } else if (format == "vm2") {
                if (mockdata)
                    throw_consistency_error("mockdata single-file line-based segments not implemented");
                else
                    res.reset(new lines::Writer(root, relname, absname));
            } else if (format == "odimh5" || format == "h5" || format == "odim") {
                throw_consistency_error("segment is a file, but odimh5 data can only be stored into directory segments");
            } else {
                if (nullptr_on_error)
                    return res;
                throw_consistency_error(
                        "getting segment for " + format + " file " + relname,
                        "format not supported");
            }
        }
        return res;
    }

    // Instantiate the right Segment implementation for a segment that already
    // exists. Returns 0 if the segment does not exist.
    std::shared_ptr<Checker> create_checker_for_existing_segment(const std::string& format, const std::string& relname, const std::string& absname, bool nullptr_on_error=false)
    {
        std::unique_ptr<struct stat> st = sys::stat(absname);
        std::shared_ptr<Checker> res;
        if (!st.get())
            st = sys::stat(absname + ".gz");
        if (!st.get())
        {
            st = sys::stat(absname + ".tar");
            if (st.get())
            {
                res.reset(new tar::Checker(root, relname, absname));
                return res;
            }
        }
        if (!st.get())
            return res;

        if (S_ISDIR(st->st_mode))
        {
            if (dir::can_store(format))
            {
                if (mockdata)
                    res.reset(new dir::HoleChecker(format, root, relname, absname));
                else
                    res.reset(new dir::Checker(format, root, relname, absname));
            } else {
                if (nullptr_on_error)
                    return res;
                throw_consistency_error(
                        "getting segment for " + format + " file " + relname,
                        "format not supported");
            }
        } else {
            if (format == "grib" || format == "grib1" || format == "grib2")
            {
                if (mockdata)
                    res.reset(new concat::HoleChecker(root, relname, absname));
                else
                    res.reset(new concat::Checker(root, relname, absname));
            } else if (format == "bufr") {
                if (mockdata)
                    res.reset(new concat::HoleChecker(root, relname, absname));
                else
                    res.reset(new concat::Checker(root, relname, absname));
            } else if (format == "vm2") {
                if (mockdata)
                    throw_consistency_error("mockdata single-file line-based segments not implemented");
                else
                    res.reset(new lines::Checker(root, relname, absname));
            } else if (format == "odimh5" || format == "h5" || format == "odim") {
                // If it's a file and we need a directory, still get a checker
                // so it can deal with it
                if (mockdata)
                    res.reset(new dir::HoleChecker(format, root, relname, absname));
                else
                    res.reset(new dir::Checker(format, root, relname, absname));
            } else {
                if (nullptr_on_error)
                    return res;
                throw_consistency_error(
                        "getting segment for " + format + " file " + relname,
                        "format not supported");
            }
        }
        return res;
    }

    Pending repack(const std::string& relname, metadata::Collection& mds, unsigned test_flags=0)
    {
        string format = utils::get_format(relname);
        string absname = str::joinpath(root, relname);
        auto maint = create_checker_for_format(format, relname, absname);
        return maint->repack(root, mds, test_flags);
    }

    State check(dataset::Reporter& reporter, const std::string& ds, const std::string& relname, const metadata::Collection& mds, bool quick=true)
    {
        string format = utils::get_format(relname);
        string absname = str::joinpath(root, relname);
        auto maint = create_checker_for_existing_segment(format, relname, absname, true);
        if (!maint)
            return SEGMENT_MISSING;
        return maint->check(reporter, ds, mds, quick);
    }

    void test_truncate(const std::string& relname, size_t offset)
    {
        string format = utils::get_format(relname);
        string absname = str::joinpath(root, relname);
        auto maint(create_checker_for_format(format, relname, absname));
        return maint->test_truncate(offset);
    }
};

/// Segment manager that picks the right readers/writers based on file types
struct AutoManager : public BaseManager
{
    AutoManager(const std::string& root, bool mockdata=false)
        : BaseManager(root, mockdata) {}

    std::shared_ptr<Writer> create_writer_for_format(const std::string& format, const std::string& relname, const std::string& absname)
    {
        auto res(create_writer_for_existing_segment(format, relname, absname));
        if (res) return res;

        if (format == "grib" || format == "grib1" || format == "grib2")
        {
            if (mockdata)
                res.reset(new concat::HoleWriter(root, relname, absname));
            else
                res.reset(new concat::Writer(root, relname, absname));
        } else if (format == "bufr") {
            if (mockdata)
                res.reset(new concat::HoleWriter(root, relname, absname));
            else
                res.reset(new concat::Writer(root, relname, absname));
        } else if (format == "odimh5" || format == "h5" || format == "odim") {
            if (mockdata)
                res.reset(new dir::HoleWriter(format, root, relname, absname));
            else
                res.reset(new dir::Writer(format, root, relname, absname));
        } else if (format == "vm2") {
            if (mockdata)
                throw_consistency_error("mockdata single-file line-based segments not implemented");
            else
                res.reset(new lines::Writer(root, relname, absname));
        } else {
            throw_consistency_error(
                    "getting writer for " + format + " file " + relname,
                    "format not supported");
        }
        return res;
    }

    std::shared_ptr<Checker> create_checker_for_format(const std::string& format, const std::string& relname, const std::string& absname)
    {
        auto res(create_checker_for_existing_segment(format, relname, absname));
        if (res) return res;

        if (format == "grib" || format == "grib1" || format == "grib2")
        {
            if (mockdata)
                res.reset(new concat::HoleChecker(root, relname, absname));
            else
                res.reset(new concat::Checker(root, relname, absname));
        } else if (format == "bufr") {
            if (mockdata)
                res.reset(new concat::HoleChecker(root, relname, absname));
            else
                res.reset(new concat::Checker(root, relname, absname));
        } else if (format == "odimh5" || format == "h5" || format == "odim") {
            if (mockdata)
                res.reset(new dir::HoleChecker(format, root, relname, absname));
            else
                res.reset(new dir::Checker(format, root, relname, absname));
        } else if (format == "vm2") {
            if (mockdata)
                throw_consistency_error("mockdata single-file line-based segments not implemented");
            else
                res.reset(new lines::Checker(root, relname, absname));
        } else {
            throw_consistency_error(
                    "getting writer for " + format + " file " + relname,
                    "format not supported");
        }
        return res;
    }

    bool exists(const std::string& relpath) const override
    {
        string abspath = str::joinpath(root, relpath);
        auto st = sys::stat(abspath);
        if (!st)
        {
            st = sys::stat(abspath + ".gz");
            return st && S_ISREG(st->st_mode);
        }

        if (S_ISDIR(st->st_mode))
        {
            st = sys::stat(str::joinpath(abspath, ".sequence"));
            return st && S_ISREG(st->st_mode);
        }

        return S_ISREG(st->st_mode);
    }

    void scan_dir(std::function<void(const std::string& relname)> dest) override
    {
        // Trim trailing '/'
        string m_root = root;
        while (m_root.size() > 1 and m_root[m_root.size()-1] == '/')
            m_root.resize(m_root.size() - 1);

        files::PathWalk walker(m_root);
        walker.consumer = [&](const std::string& relpath, sys::Path::iterator& entry, struct stat& st) {
            // Skip '.', '..' and hidden files
            if (entry->d_name[0] == '.')
                return false;

            string name = entry->d_name;

            // Skip compressed data index files
            if (str::endswith(name, ".gz.idx"))
                return false;

            bool is_dir = S_ISDIR(st.st_mode);
            if (is_dir)
            {
                sys::Path sub(entry.open_path());
                struct stat seq_st;
                if (sub.fstatat_ifexists(".sequence", seq_st))
                {
                    // Directory segment
                    string format = utils::get_format(name);
                    if (dir::can_store(format))
                        dest(str::joinpath(relpath, name));
                    return false;
                }
                else
                    // Normal subdirectory, recurse into it
                    return true;
            } else {
                // Concat segment
                if (str::endswith(name, ".gz"))
                    name = name.substr(0, name.size() - 3);

                // Check whether the file format (from the extension) could be
                // stored in this kind of segment
                string format = utils::get_format(name);
                if (fd::can_store(format))
                    dest(str::joinpath(relpath, name));
                return false;
            }
        };

        walker.walk();
    }
};

/// Segment manager that always picks directory segments
struct ForceDirManager : public BaseManager
{
    ForceDirManager(const std::string& root) : BaseManager(root) {}

    std::shared_ptr<Writer> create_writer_for_format(const std::string& format, const std::string& relname, const std::string& absname) override
    {
        auto res(create_writer_for_existing_segment(format, relname, absname));
        if (res) return res;
        return std::shared_ptr<segment::Writer>(new dir::Writer(format, root, relname, absname));
    }

    std::shared_ptr<Checker> create_checker_for_format(const std::string& format, const std::string& relname, const std::string& absname) override
    {
        return std::shared_ptr<segment::Checker>(new dir::Checker(format, root, relname, absname));
    }

    bool exists(const std::string& relpath) const override
    {
        string abspath = str::joinpath(root, relpath);
        auto st = sys::stat(abspath);
        if (!st || !S_ISDIR(st->st_mode))
            return false;

        st = sys::stat(str::joinpath(abspath, ".sequence"));
        return st && S_ISREG(st->st_mode);
    }

    void scan_dir(std::function<void(const std::string& relname)> dest) override
    {
        // Trim trailing '/'
        string m_root = root;
        while (m_root.size() > 1 and m_root[m_root.size()-1] == '/')
            m_root.resize(m_root.size() - 1);

        files::PathWalk walker(m_root);
        walker.consumer = [&](const std::string& relpath, sys::Path::iterator& entry, struct stat& st) {
            // Skip '.', '..' and hidden files
            if (entry->d_name[0] == '.')
                return false;

            string name = entry->d_name;

            // Skip compressed data index files
            if (str::endswith(name, ".gz.idx"))
                return false;

            if (!S_ISDIR(st.st_mode))
                return false;

            sys::Path sub(entry.open_path());
            struct stat seq_st;
            if (!sub.fstatat_ifexists(".sequence", seq_st))
                // Normal subdirectory, recurse into it
                return true;

            // Check whether the file format (from the extension) could be
            // stored in this kind of segment
            string format = utils::get_format(name);
            if (dir::can_store(format))
                dest(str::joinpath(relpath, name));
            return false;
        };

        walker.walk();
    }
};

/// Segment manager that always uses hole file segments
struct HoleDirManager : public ForceDirManager
{
    HoleDirManager(const std::string& root) : ForceDirManager(root) {}

    std::shared_ptr<Writer> create_writer_for_format(const std::string& format, const std::string& relname, const std::string& absname) override
    {
        return std::shared_ptr<Writer>(new dir::HoleWriter(format, root, relname, absname));
    }
};

}


Manager::Manager(const std::string& root)
    : root(root)
{
}

Manager::~Manager()
{
}

std::shared_ptr<Writer> Manager::get_writer(const std::string& relname)
{
    return get_writer(utils::get_format(relname), relname);
}

std::shared_ptr<Writer> Manager::get_writer(const std::string& format, const std::string& relname)
{
    // Ensure that the directory for 'relname' exists
    string absname = str::joinpath(root, relname);
    size_t pos = absname.rfind('/');
    if (pos != string::npos)
        sys::makedirs(absname.substr(0, pos));

    // Refuse to write to compressed files
    if (scan::isCompressed(absname))
        throw_consistency_error("accessing data file " + relname,
                "cannot update compressed data files: please manually uncompress it first");

    // Else we need to create an appropriate one
    return create_writer_for_format(format, relname, absname);
}

std::shared_ptr<Checker> Manager::get_checker(const std::string& relname)
{
    return get_checker(utils::get_format(relname), relname);
}

std::shared_ptr<Checker> Manager::get_checker(const std::string& format, const std::string& relname)
{
    string absname = str::joinpath(root, relname);
    return create_checker_for_format(format, relname, absname);
}

std::unique_ptr<Manager> Manager::get(const std::string& root, bool force_dir, bool mock_data)
{
    if (force_dir)
        if (mock_data)
            return unique_ptr<Manager>(new HoleDirManager(root));
        else
            return unique_ptr<Manager>(new ForceDirManager(root));
    else
        return unique_ptr<Manager>(new AutoManager(root, mock_data));
}

}
}
}
