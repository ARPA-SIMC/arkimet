#ifndef ARKI_UTILS_GEOSDEF_H
#define ARKI_UTILS_GEOSDEF_H

/*
 * C++ Wrapper for the stable C API of GEOS.
 *
 * This wraps the thread-safe C API, internally using a thread_local context.
 *
 * The stable C API of GEOS is itself a wrapper for the C++ GEOS API, so it
 * would be easier and more efficient to use the C++ API directly. However, on
 * top of API/ABI stability issues, some version of the C++ API have been
 * packaged intentionally broken (see https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=1010002 ),
 * so wrapping the C API fixes both problems.
 */

#include <arki/utils/geosfwd.h>

#ifdef HAVE_GEOS
#include <vector>
#include <string>
#include <geos_c.h>
#endif

namespace arki {
namespace utils {
namespace geos {

#ifdef HAVE_GEOS

class GEOSError : public std::exception
{
    std::string msg;

public:
    GEOSError();
    const char* what() const noexcept override;
};


class Context
{
    GEOSContextHandle_t ctx;

public:
    Context();
    ~Context();
    Context(const Context&) = delete;
    Context(Context&&) = delete;

    Context& operator=(const Context&) = delete;
    Context& operator=(Context&&) = delete;

    operator GEOSContextHandle_t() { return ctx; }
};


class GeometryVector : protected std::vector<GEOSGeometry*>
{
public:
    using std::vector<GEOSGeometry*>::vector;
    using std::vector<GEOSGeometry*>::push_back;
    using std::vector<GEOSGeometry*>::empty;
    using std::vector<GEOSGeometry*>::data;
    using std::vector<GEOSGeometry*>::size;

    ~GeometryVector();

    void emplace_back(Geometry&&);

    void discard() { clear(); }
};


template<typename Pointer, void Deleter(GEOSContextHandle_t, Pointer)>
class Wrapper
{
protected:
    Pointer ptr = nullptr;

public:
    Wrapper() = default;
    Wrapper(Pointer ptr) : ptr(ptr) {}
    Wrapper(const Wrapper&) = delete;
    Wrapper(Wrapper&& o) : ptr(o.ptr)
    {
        o.ptr = nullptr;
    }
    ~Wrapper();

    Wrapper& operator=(Pointer o);
    Wrapper& operator=(const Wrapper& o) = delete;
    Wrapper& operator=(Wrapper&& o);

    Pointer release()
    {
        auto res = ptr;
        ptr = nullptr;
        return res;
    }

    operator bool() { return ptr; }
    operator Pointer() { return ptr; }
    operator const Pointer() const { return ptr; }
};


class CoordinateSequence : public Wrapper<GEOSCoordSequence*, GEOSCoordSeq_destroy_r>
{
public:
    using Wrapper::Wrapper;
    using Wrapper::operator=;

    CoordinateSequence(unsigned size, unsigned dims=2);

    void setxy(unsigned idx, double x, double y);
};


class Geometry : public Wrapper<GEOSGeometry*, GEOSGeom_destroy_r>
{
public:
    using Wrapper::Wrapper;
    using Wrapper::operator=;

    Geometry clone() const;

    Geometry convex_hull() const;

    bool equals(const Geometry& o) const;
    bool intersects(const Geometry& o) const;
    bool covers(const Geometry& o) const;
    bool covered_by(const Geometry& o) const;

    static Geometry create_point(double lat, double lon);
    static Geometry create_linear_ring(CoordinateSequence&& seq);
    static Geometry create_polygon(Geometry&& exterior, GeometryVector&& interior);
    static Geometry create_collection(GeometryVector&& geoms);
};


class WKTReader : public Wrapper<GEOSWKTReader*, GEOSWKTReader_destroy_r>
{
public:
    using Wrapper::Wrapper;
    using Wrapper::operator=;

    WKTReader();

    Geometry read(const char* str);
    Geometry read(const std::string& str);
};


class WKTWriter : public Wrapper<GEOSWKTWriter*, GEOSWKTWriter_destroy_r>
{
public:
    using Wrapper::Wrapper;
    using Wrapper::operator=;

    WKTWriter();

    std::string write(const Geometry& g);
};

#else

struct Geometry {};
struct WKTReader {};
struct WKTWriter {};

extern template class Wrapper<GEOSGeometry*, GEOSGeom_destroy_r>;
extern template class Wrapper<GEOSWKTReader*, GEOSWKTReader_destroy_r>;
extern template class Wrapper<GEOSWKTWriter*, GEOSWKTWriter_destroy_r>;

#endif

}
}
}

#endif
