#include "geos.h"
#include "arki/nag.h"

namespace arki {
namespace utils {
namespace geos {

static thread_local Context context;
static thread_local std::string last_error;

static void geos_notice_handler(const char* fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    nag::warning(fmt, ap);
    va_end(ap);
}

static void geos_error_handler(const char* fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);

    // Use a copy of ap to compute the size, since a va_list can be iterated
    // only once
    va_list ap1;
    va_copy(ap1, ap);
    auto size = vsnprintf(nullptr, 0, fmt, ap1);
    va_end(ap1);

    last_error.resize(size + 1);
    // TODO: remove the const cast when we have C++17
    vsnprintf(const_cast<char*>(last_error.data()), size + 1, fmt, ap);
    last_error.resize(size);
}


GEOSError::GEOSError()
    : msg(last_error)
{
    if (msg.empty())
        msg = "GEOS returned an error code but no logged error message can be found to explain it";
}

const char* GEOSError::what() const noexcept
{
    return msg.c_str();
}


#if GEOS_VERSION_MAJOR > 3 || (GEOS_VERSION_MAJOR == 3 && GEOS_VERSION_MINOR >= 5)
Context::Context()
    : ctx(GEOS_init_r())
{
    GEOSContext_setNoticeHandler_r(ctx, geos_notice_handler);
    GEOSContext_setErrorHandler_r(ctx, geos_error_handler);
}
#else
Context::Context()
    : ctx(initGEOS_r(geos_notice_handler, geos_error_handler))
{
}
#endif

Context::~Context()
{
#if GEOS_VERSION_MAJOR > 3 || (GEOS_VERSION_MAJOR == 3 && GEOS_VERSION_MINOR >= 5)
    GEOS_finish_r(ctx);
#else
    finishGEOS_r(ctx);
#endif
}


GeometryVector::~GeometryVector()
{
    for (auto g: *this)
        GEOSGeom_destroy_r(context, g);
}

void GeometryVector::emplace_back(Geometry&& g)
{
    std::vector<GEOSGeometry*>::emplace_back(g.release());
}


template<typename Pointer, void Deleter(GEOSContextHandle_t, Pointer)>
Wrapper<Pointer, Deleter>::~Wrapper()
{
    if (ptr)
        Deleter(context, ptr);
}


template<typename Pointer, void Deleter(GEOSContextHandle_t, Pointer)>
Wrapper<Pointer, Deleter>& Wrapper<Pointer, Deleter>::operator=(Pointer o)
{
    // Prevent damage on assignment of same pointer
    if (ptr == o) return *this;

    if (ptr)
    {
        Deleter(context, ptr);
    }

    ptr = o;

    return *this;
}

template<typename Pointer, void Deleter(GEOSContextHandle_t, Pointer)>
Wrapper<Pointer, Deleter>& Wrapper<Pointer, Deleter>::operator=(Wrapper&& o)
{
    // Prevent damage on assignment to self
    if (&o == this) return *this;

    if (ptr && o.ptr != ptr)
    {
        Deleter(context, ptr);
    }

    ptr = o.ptr;
    o.ptr = nullptr;

    return *this;
}


CoordinateSequence::CoordinateSequence(unsigned size, unsigned dims)
{
    ptr = GEOSCoordSeq_create_r(context, size, dims);
    if (!ptr)
        throw GEOSError();
}

void CoordinateSequence::setxy(unsigned idx, double x, double y)
{
#if GEOS_VERSION_MAJOR > 3 || (GEOS_VERSION_MAJOR == 3 && GEOS_VERSION_MINOR >= 8)
    int res = GEOSCoordSeq_setXY_r(context, ptr, idx, x, y);
    if (!res)
        throw GEOSError();
#else
    int res = GEOSCoordSeq_setX_r(context, ptr, idx, x);
    if (!res)
        throw GEOSError();
    res = GEOSCoordSeq_setY_r(context, ptr, idx, y);
    if (!res)
        throw GEOSError();
#endif
}


Geometry Geometry::clone() const
{
    if (!ptr)
        return Geometry();

    Geometry res(GEOSGeom_clone_r(context, ptr));
    if (!res)
        throw GEOSError();
    return res;
}

Geometry Geometry::convex_hull() const
{
    if (!ptr)
        return Geometry();

    Geometry res(GEOSConvexHull_r(context, ptr));
    if (!res)
        throw GEOSError();
    return res;
}

bool Geometry::equals(const Geometry& o) const
{
    char res = GEOSEquals_r(context, ptr, o);
    if (res == 2)
        throw GEOSError();
    return res;
}

bool Geometry::intersects(const Geometry& o) const
{
    char res = GEOSIntersects_r(context, ptr, o);
    if (res == 2)
        throw GEOSError();
    return res;
}

bool Geometry::covers(const Geometry& o) const
{
    char res = GEOSCovers_r(context, ptr, o);
    if (res == 2)
        throw GEOSError();
    return res;
}

bool Geometry::covered_by(const Geometry& o) const
{
    char res = GEOSCoveredBy_r(context, ptr, o);
    if (res == 2)
        throw GEOSError();
    return res;
}

Geometry Geometry::create_point(double lat, double lon)
{
    CoordinateSequence seq(1);
    seq.setxy(0, lon, lat);

    Geometry res(GEOSGeom_createPoint_r(context, seq.release()));
    if (!res)
        throw GEOSError();

    return res;
}

Geometry Geometry::create_linear_ring(CoordinateSequence&& seq)
{
    Geometry res(GEOSGeom_createLinearRing_r(context, seq.release()));
    if (!res)
        throw GEOSError();

    return res;
}

Geometry Geometry::create_polygon(Geometry&& exterior, GeometryVector&& interior)
{
    Geometry res(GEOSGeom_createPolygon_r(context, exterior.release(), interior.data(), interior.size()));
    if (!res)
        throw GEOSError();

    interior.discard();
    return res;
}

Geometry Geometry::create_collection(GeometryVector&& geoms)
{
    Geometry res(GEOSGeom_createCollection_r(context, GEOS_GEOMETRYCOLLECTION, geoms.data(), geoms.size()));
    if (!res)
        throw GEOSError();

    geoms.discard();

    return res;
}


WKTReader::WKTReader()
{
    ptr = GEOSWKTReader_create_r(context);
}

Geometry WKTReader::read(const char* str)
{
    Geometry res(GEOSWKTReader_read_r(context, ptr, str));
    if (!res)
        throw GEOSError();
    return res;
}

Geometry WKTReader::read(const std::string& str)
{
    Geometry res(GEOSWKTReader_read_r(context, ptr, str.c_str()));
    if (!res)
        throw GEOSError();
    return res;
}


WKTWriter::WKTWriter()
{
    ptr = GEOSWKTWriter_create_r(context);
}

std::string WKTWriter::write(const Geometry& g)
{
    char* wkt = GEOSWKTWriter_write_r(context, ptr, g);
    if (!wkt)
        throw GEOSError();

    // Rewrap into an std::string
    std::string res(wkt);
    GEOSFree_r(context, wkt);
    return res;
}


template class Wrapper<GEOSGeometry*, GEOSGeom_destroy_r>;
template class Wrapper<GEOSWKTReader*, GEOSWKTReader_destroy_r>;
template class Wrapper<GEOSWKTWriter*, GEOSWKTWriter_destroy_r>;

}
}
}
