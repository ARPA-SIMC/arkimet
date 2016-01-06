#ifndef ARKI_FILE_H
#define ARKI_FILE_H

#include <arki/utils/sys.h>
#include <string>

namespace arki {

/*
 * NamedFileDescriptor and File come from wobble and moving their
 * implementation outside of utils means making it hard to update it in the
 * future.
 *
 * This header exists to promote NamedFileDescriptor and File to non-utils arki
 * namespace, so they can be used by general arkimet APIs.
 */
using arki::utils::sys::NamedFileDescriptor;
using arki::utils::sys::File;

struct Stdin : public NamedFileDescriptor
{
    Stdin();
};

struct Stdout : public NamedFileDescriptor
{
    Stdout();
};

struct Stderr : public NamedFileDescriptor
{
    Stderr();
};


/**
 * Abstract interface to things that return a line of text at a time
 */
struct LineReader
{
    virtual ~LineReader() {}

    /**
     * Similar to std::getline, raises an exception if anything went wrong.
     *
     * Returns false if EOF is reached, in which case line will be empty.
     */
    virtual bool getline(std::string& line) = 0;

    /**
     * Check if the last getline returned eof. This is always false before
     * getline is called for the first time.
     */
    virtual bool eof() const = 0;

    /**
     * Create a LineReader from a file descriptor.
     *
     * The file descriptor is not managed by the LineReader, and will ned to be
     * kept open by the caller for as long as the line reader is used, then closed
     * at the end.
     *
     * Note that a LineReader on a file descriptor needs to do read ahead to avoid
     * reading one character at a time, so if the caller stops calling getline(),
     * the file descriptor is likely to be positioned further ahead than the last
     * line read.
     */
    static std::unique_ptr<LineReader> from_fd(NamedFileDescriptor& fd);

    /**
     * Create a LineReader from a buffer on a string.
     *
     * No copy is made of the data: the buffer must remain valid for as long as the
     * line reader is used.
     */
    static std::unique_ptr<LineReader> from_chars(const char* buf, size_t size);
};

}
#endif
