/*
 * Copyright (C) 2012--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Emanuele Di Giacomo <edigiacomo@arpa.emr.it>
 */

#include <arki/metadata/tests.h>
#include <arki/metadata/collection.h>
#include <arki/types/value.h>
#include <arki/scan/vm2.h>
#include <arki/scan/any.h>
#include <wibble/sys/fs.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace tut {
using namespace std;
using namespace wibble;
using namespace wibble::tests;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;

struct arki_scan_vm2_shar {
};
TESTGRP(arki_scan_vm2);

// Scan a well-known vm2 sample
template<> template<>
void to::test<1>()
{
    Metadata md;
    scan::Vm2 scanner;
    types::Time reftime;
    wibble::sys::Buffer buf;

    scanner.open("inbound/test.vm2");
    // See how we scan the first vm2
    ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("vm2", sys::fs::abspath("."), "inbound/test.vm2", 0, 34));

    // Check contents
    wassert(actual(md).contains("area", "VM2(1)"));
    wassert(actual(md).contains("product", "VM2(227)"));
    wassert(actual(md).contains("reftime", "1987-10-31T00:00:00Z"));
    wassert(actual(md).contains("value", "1.2,,,000000000"));

    // Check that the source can be read properly
    md.unset(TYPE_VALUE);
    md.drop_cached_data();
    buf = md.getData();
    ensure_equals(buf.size(), 34u);
    ensure_equals(string((const char*)buf.data(), 34), "198710310000,1,227,1.2,,,000000000");
}

template<> template<>
void to::test<2>()
{
    Metadata md;
    wibble::sys::Buffer buf;

    const scan::Validator& v = scan::vm2::validator();

    int fd = open("inbound/test.vm2", O_RDONLY);

#define ensure_no_throws(x) do { try { x; } catch(wibble::exception::Generic& e) { ensure(false); } } while (0)
#define ensure_throws(x) do { try { x; ensure(false); } catch (wibble::exception::Generic& e) { } } while (0)

    ensure_no_throws(v.validate(fd, 0, 35, "inbound/test.vm2"));
    ensure_no_throws(v.validate(fd, 0, 34, "inbound/test.vm2"));
    ensure_no_throws(v.validate(fd, 35, 35, "inbound/test.vm2"));
    ensure_no_throws(v.validate(fd, 35, 36, "inbound/test.vm2"));

    ensure_throws(v.validate(fd, 1, 35, "inbound/test.vm2"));
    ensure_throws(v.validate(fd, 0, 36, "inbound/test.vm2"));
    ensure_throws(v.validate(fd, 34, 34, "inbound/test.vm2"));
    ensure_throws(v.validate(fd, 36, 34, "inbound/test.vm2"));

    close(fd);

    metadata::Collection mdc;
    scan::scan("inbound/test.vm2", mdc);
    mdc[0].unset(types::TYPE_VALUE);
    buf = mdc[0].getData();

    v.validate(buf.data(), buf.size());
    ensure_throws(v.validate((const char*)buf.data()+1, buf.size()-1));

    std::ifstream in("inbound/test.vm2");
    std::string line;
    while (std::getline(in, line)) {
        line += "\n";
        ensure_no_throws(v.validate(line.c_str(), line.size()));
    }
}

// Scan a well-known vm2 sample (with seconds)
template<> template<>
void to::test<3>()
{
    Metadata md;
    scan::Vm2 scanner;
    types::Time reftime;
    wibble::sys::Buffer buf;

    scanner.open("inbound/test.vm2");
    // Skip the first vm2
    ensure(scanner.next(md));

    // See how we scan the second vm2
    ensure(scanner.next(md));

    // Check the source info
    wassert(actual(md.source().cloneType()).is_source_blob("vm2", sys::fs::abspath("."), "inbound/test.vm2", 35, 35));

    // Check contents
    wassert(actual(md).contains("area", "VM2(1)"));
    wassert(actual(md).contains("product", "VM2(228)"));
    wassert(actual(md).contains("reftime", "1987-10-31T00:00:30Z"));
    wassert(actual(md).contains("value", ".5,,,000000000"));

    // Check that the source can be read properly
    md.unset(TYPE_VALUE);
    md.drop_cached_data();
    buf = md.getData();
    ensure_equals(buf.size(), 35u);
    ensure_equals(string((const char*)buf.data(), 35), "19871031000030,1,228,.5,,,000000000");
}

// Scan and reconstruct a VM2 sample
template<> template<>
void to::test<4>()
{
    const types::Value* value;
    wibble::sys::Buffer buf;

    metadata::Collection mdc;
    scan::scan("inbound/test.vm2", mdc);

    value = mdc[0].get<types::Value>();
    buf = scan::Vm2::reconstruct(mdc[0], value->buffer);
    wassert(actual(string((const char*)buf.data(), buf.size())) == "198710310000,1,227,1.2,,,000000000");

    value = mdc[1].get<types::Value>();
    buf = scan::Vm2::reconstruct(mdc[1], value->buffer);
    wassert(actual(string((const char*)buf.data(), buf.size())) == "19871031000030,1,228,.5,,,000000000");
}

// Scan a corrupted VM2
template<> template<>
void to::test<5>()
{
    system("cp inbound/test.vm2 inbound/test-corrupted.vm2");
    system("dd if=/dev/zero of=inbound/test-corrupted.vm2 bs=1 seek=71 count=33 conv=notrunc 2>/dev/null");

    metadata::Collection mdc;
    scan::scan("inbound/test-corrupted.vm2", mdc);

    wassert(actual(mdc.size()) == 3);

    // Check the source info
    wassert(actual(mdc[0].source().cloneType()).is_source_blob("vm2", sys::fs::abspath("."), "inbound/test-corrupted.vm2", 0, 34));
    wassert(actual(mdc[1].source().cloneType()).is_source_blob("vm2", sys::fs::abspath("."), "inbound/test-corrupted.vm2", 35, 35));
    wassert(actual(mdc[2].source().cloneType()).is_source_blob("vm2", sys::fs::abspath("."), "inbound/test-corrupted.vm2", 105, 32));

    system("rm inbound/test-corrupted.vm2");
}

}

// vim:set ts=4 sw=4:
