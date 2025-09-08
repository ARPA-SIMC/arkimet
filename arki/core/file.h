#ifndef ARKI_CORE_FILE_H
#define ARKI_CORE_FILE_H

#include <arki/core/fwd.h>
#include <arki/utils/sys.h>
#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace arki::core {

/*
 * NamedFileDescriptor and File come from wobble and moving their
 * implementation outside of utils means making it hard to update it in the
 * future.
 *
 * This header exists to promote NamedFileDescriptor and File to non-utils arki
 * namespace, so they can be used by general arkimet APIs.
 */
using arki::utils::sys::File;
using arki::utils::sys::NamedFileDescriptor;

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
 * Abstract input file implementation to use for output operation on things
 * that are not file descriptors
 */
struct AbstractInputFile
{
    virtual ~AbstractInputFile();

    [[deprecated("Use path() instead")]] virtual std::string name() const
    {
        return path().native();
    }
    virtual std::filesystem::path path() const = 0;

    /**
     * Read up to \a size bytes into dest.
     *
     * Return the number of bytes read, or 0 for EOF
     */
    virtual size_t read(void* dest, size_t size) = 0;
};

class MemoryFile : public AbstractInputFile
{
protected:
    std::filesystem::path m_path;
    const void* buffer;
    size_t buffer_size;
    size_t pos = 0;

public:
    MemoryFile(const void* buffer, size_t size,
               const std::filesystem::path& path);

    std::filesystem::path path() const override { return m_path; }

    size_t read(void* dest, size_t size) override;
};

/**
 * Generic abstract input interface, with buffering
 */
struct BufferedReader
{
protected:
    std::array<uint8_t, 65536> buffer;
    unsigned pos = 0;
    unsigned end = 0;

    /**
     * Assume the buffer is empty. Read into the buffer, returning the number
     * of bytes read.
     *
     * Return 0 in case end-of-file is reached.
     */
    virtual unsigned read() = 0;

    /**
     * Call read() to refill an empty input buffer.
     *
     * Return false if end-of-file is reached.
     */
    bool refill();

public:
    virtual ~BufferedReader();

    /**
     * Return the next character in the input buffer, or EOF if we are at the
     * end of file.
     *
     * Does not advance the current position
     */
    int peek();

    /**
     * Return the next character in the input buffer, or EOF if we are at the
     * end of file.
     *
     * It advances the current position
     */
    int get();

    /**
     * Create a BufferedReader from a file descriptor.
     *
     * The file descriptor is not managed by the BufferedReader, and will need
     * to be kept open by the caller for as long as the reader is used, then
     * closed at the end.
     *
     * Note that a BufferedReader reads data in chunks from its input, so if
     * the caller stops calling get(), the file descriptor is likely to be
     * positioned further ahead than the character read
     */
    static std::unique_ptr<BufferedReader> from_fd(NamedFileDescriptor& fd);

    /**
     * Create a BufferedReader from an abstract input file.
     *
     * The file descriptor is not managed by the BufferedReader, and will ned
     * to be kept open by the caller for as long as the reader is used, then
     * closed at the end.
     *
     * Note that a BufferedReader reads data in chunks from its input, so if
     * the caller stops calling get(), the file descriptor is likely to be
     * positioned further ahead than the character read
     */
    static std::unique_ptr<BufferedReader> from_abstract(AbstractInputFile& fd);

    /**
     * Create a BufferedReader from a buffer on a string.
     *
     * No copy is made of the data: the buffer must remain valid for as long as
     * the reader is used.
     */
    static std::unique_ptr<BufferedReader> from_chars(const char* buf,
                                                      size_t size);

    /**
     * Create a BufferedReader from a buffer on a string.
     *
     * No copy is made of the data: the buffer must remain valid for as long as
     * the reader is used.
     */
    static std::unique_ptr<BufferedReader> from_string(const std::string& str);
};

/**
 * Abstract interface to things that return a line of text at a time
 */
class LineReader
{
protected:
    bool fd_eof = false;

public:
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
    bool eof() const { return fd_eof; }

    /**
     * Create a LineReader from a file descriptor.
     *
     * The file descriptor is not managed by the LineReader, and will ned to be
     * kept open by the caller for as long as the line reader is used, then
     * closed at the end.
     *
     * Note that a LineReader on a file descriptor needs to do read ahead to
     * avoid reading one character at a time, so if the caller stops calling
     * getline(), the file descriptor is likely to be positioned further ahead
     * than the last line read.
     */
    static std::unique_ptr<LineReader> from_fd(NamedFileDescriptor& fd);

    /**
     * Create a LineReader from an abstract input file.
     *
     * The file descriptor is not managed by the LineReader, and will ned to be
     * kept open by the caller for as long as the line reader is used, then
     * closed at the end.
     *
     * Note that a LineReader on a file descriptor needs to do read ahead to
     * avoid reading one character at a time, so if the caller stops calling
     * getline(), the file descriptor is likely to be positioned further ahead
     * than the last line read.
     */
    static std::unique_ptr<LineReader> from_abstract(AbstractInputFile& fd);

    /**
     * Create a LineReader from a buffer on a string.
     *
     * No copy is made of the data: the buffer must remain valid for as long as
     * the line reader is used.
     */
    static std::unique_ptr<LineReader> from_chars(const char* buf, size_t size);

    /**
     * Create a LineReader from a buffer on a string.
     *
     * No copy is made of the data: the buffer must remain valid for as long as
     * the reader is used.
     */
    static std::unique_ptr<LineReader> from_string(const std::string& str);
};

} // namespace arki::core
#endif
