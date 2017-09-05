Summary: Archive for weather information
Name: arkimet
Version: 1.4
Release: 1
License: GPL
Group: Applications/Meteo
URL: https://github.com/arpa-simc/%{name}
Source0: https://github.com/arpa-simc/%{name}/archive/v%{version}-%{release}.tar.gz#/%{name}-%{version}-%{release}.tar.gz
Source1: https://github.com/arpa-simc/%{name}/raw/v%{version}-%{release}/fedora/SOURCES/%{name}.service
Source2: https://github.com/arpa-simc/%{name}/raw/v%{version}-%{release}/fedora/SOURCES/%{name}.sysconfig
Source3: https://github.com/arpa-simc/%{name}/raw/v%{version}-%{release}/fedora/SOURCES/%{name}-logrotate.conf
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
%if 0%{?rhel} == 7
%define python3_vers python34
%else
%define python3_vers python3
%endif
BuildRequires: doxygen, libdballe-devel >= 5.19, lua-devel >= 5.1, grib_api-devel, sqlite-devel >= 3.6, curl-devel, geos-devel, popt-devel, help2man, pkgconfig, readline-devel, lzo-devel, libwreport-devel >= 3.0, flex, bison, meteo-vm2-devel >= 0.12, hdf5-devel, %{python3_vers}, %{python3_vers}-devel, %{python3_vers}-werkzeug, %{python3_vers}-setproctitle, %{python3_vers}-nose
Requires: hdf5, meteo-vm2 >= 0.12, grib_api, %{python3_vers}, %{python3_vers}-lxml, %{python3_vers}-werkzeug, %{python3_vers}-setproctitle, libdballe6, systemd

%{!?python3_sitelib: %define python3_sitelib %(%{__python3} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}
%{!?python3_sitearch: %define python3_sitearch %(%{__python3} -c "from distutils.sysconfig import get_python_lib; print get_python_lib(1)")}

%description
Arkimet is a set of tools to organize, archive and distribute 
data files.
It currently supports data in GRIB, BUFR, HDF5 and VM2 formats. 

Arkimet manages a set of datasets, each of which contains 
omogeneous data. It exploits the commonalities between the data 
in a dataset to implement a fast, powerful and space-efficient 
indexing system. 
When data is ingested into arkimet, it is scanned and annotated 
with metadata, such as reference time and product information, 
then it is dispatched to one of the datasets configured in the 
system. 
Datasets can be queried with a comprehensive query language, 
both locally and over the network using an HTTP-based protocol. 
Old data can be archived using an automatic maintenance procedure,
and archives can be taken offline and back online at will. 
A summary of offline data is kept online, so that arkimet is able 
to report that more data for a query would be available but is 
currently offline.

%package  -n arkimet-devel
Summary:  Arkimet developement library
Group:    Applications/Meteo
Requires: libdballe-devel, grib_api-devel, libwreport-devel, %{python3_vers}-devel, meteo-vm2-devel, hdf5-devel, sqlite-devel, curl-devel

%description -n arkimet-devel
 Arkimet developement library

%prep
%setup -q -n %{name}-%{version}-%{release}
sh autogen.sh

%build
%configure --enable-arpae-tests
make

# unit tests arki_dataset_concurrent_* fail on f20 because of the fallback on
# per-process locks instead of per-file-descriptor locks.
# See issue https://github.com/ARPAE-SIMC/arkimet/issues/60
%if 0%{?fedora} >= 24
make check
%endif


%install
[ "%{buildroot}" != / ] && rm -rf %{buildroot}
%makeinstall

install -D -m0644 %{SOURCE1} %{buildroot}%{_unitdir}/%{name}.service
install -bD -m0644 %{SOURCE2} %{buildroot}%{_sysconfdir}/sysconfig/%{name}
install -D -m 0644 -p %{SOURCE3} %{buildroot}%{_sysconfdir}/logrotate.d/%{name}

%clean
[ "%{buildroot}" != / ] && rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%dir %{_sysconfdir}/arkimet
%config(noreplace) %{_sysconfdir}/arkimet/match-alias.conf
%{_sysconfdir}/arkimet/bbox/*
%{_sysconfdir}/arkimet/format/*
%{_sysconfdir}/arkimet/qmacro/*
%{_sysconfdir}/arkimet/report/*
%{_sysconfdir}/arkimet/scan-bufr/*
%{_sysconfdir}/arkimet/scan-grib1/*
%{_sysconfdir}/arkimet/scan-grib2/*
%{_sysconfdir}/arkimet/scan-odimh5/*
%{_sysconfdir}/arkimet/targetfile/*
%{_sysconfdir}/arkimet/vm2/*
%{_bindir}/*
%{_libdir}/libarkimet.so.*
%{_unitdir}/%{name}.service
%config(noreplace) %{_sysconfdir}/sysconfig/arkimet
%config(noreplace) %{_sysconfdir}/logrotate.d/arkimet
%dir %{python3_sitelib}/arkimet
%{python3_sitelib}/arkimet/*
%dir %{python3_sitearch}
%{python3_sitearch}/*.a
%{python3_sitearch}/*.la
%{python3_sitearch}/*.so*
%doc %{_mandir}/man1/*
%doc README.md
%doc %{_docdir}/arkimet/*

%files -n arkimet-devel
%defattr(-,root,root,-)
%{_libdir}/libarkimet*.a
%{_libdir}/libarkimet*.la
%{_libdir}/libarkimet.so
%dir %{_includedir}/arki
%{_includedir}/arki/*

%pre
if [ "$1" = "1"]; then
    # it's an initial installation, not an upgrade
    /usr/bin/getent passwd USERNAME >/dev/null \
        || echo "Warning: arkimet user not present in this system, running an arki-server will need additional configuration"
fi

%post
/sbin/ldconfig

%preun

%postun
/sbin/ldconfig
/usr/bin/systemctl daemon-reload
if [ "$1" = "1" ]; then
    # it's an upgrade, not a uninstall
    /usr/bin/systemctl try-restart %{name}
fi

%changelog
* Mon Sep 4 2017 Daniele Branchini <dbranchini@arpae.it> - 1.4-1%{dist}
- Fixed #65, #73, #75, #79, #80, #87, #93, #100

* Mon Jun 26 2017 Daniele Branchini <dbranchini@arpae.it> - 1.3-2%{dist}
- Added logrotate conf (#98)

* Thu Jun 22 2017 Daniele Branchini <dbranchini@arpae.it> - 1.3-1%{dist}
- Fixed issues in arki-check (#90, #91)
- Use exception.code in response (if possible)
- Test code cleanup
    
* Wed Jun 7 2017 Daniele Branchini <dbranchini@arpae.it> - 1.2-1%{dist}
- Added documentation for dataset formats and check/fix/repack procedures (#88)
- Added verbose reporting to vacuum operations, and some test refactoring (#89)
- HTTP authentication method (any) and optional .netrc (#81)

* Thu Apr 6 2017 Daniele Branchini <dbranchini@arpae.it> - 1.1-2%{dist}
- fixed #83

* Tue Feb 14 2017 Daniele Branchini <dbranchini@arpae.it> - 1.1-1%{dist}
- New dataset format: one index per segment (#60)

* Tue Jan 3 2017 Daniele Branchini <dbranchini@arpae.it> - 1.0-18%{dist}
- fixed #63, #64, #66, #68, #70

* Thu Dec 15 2016 Daniele Branchini <dbranchini@arpae.it> - 1.0-17%{dist}
- fixed #9, #61, #62

* Tue Oct 18 2016 Daniele Branchini <dbranchini@arpae.it> - 1.0-16%{dist}
- fixed #55, #56, #57, #43, #48
- fixed #54 (implemented --copyok/copyko)

* Mon Sep 26 2016 Daniele Branchini <dbranchini@arpae.it> - 1.0-15%{dist}
- fixed #53
- fixed #51

* Tue Sep 20 2016 Daniele Branchini <dbranchini@arpae.it> - 1.0-14%{dist}
- fixed dependencies
- commented out directive in systemd script that caused fedora20 server to hang

* Tue Sep 13 2016 Daniele Branchini <dbranchini@arpae.it> - 1.0-13%{dist}
- fixed #49
- implemented output format `arki-server --journald`
- implemented output format `arki-server --perflog`

* Wed Sep 7 2016 Daniele Branchini <dbranchini@arpae.it> - 1.0-12%{dist}
- fixed #47
- implemented `arki-check --unarchive=segment`, to move a segment from
    `.archives/last` back to the main dataset.
- removed wibble dependency
- fine tuning of systemd scripts
- implemented sharded datasets: in a dataset configuration, use
    `shard=yearly`, `shard=monthly`, or `shard=weekly`, to have the dataset
    create subdatasets where data will be saved. Locking will only happen at
    the subdataset level.
- implemented `arki-check --state` to show the computed state of each segment
    in a dataset.

* Wed Aug 10 2016 Davide Cesari <dcesari@arpae.it> - 1.0-10%{dist}
- switch to systemd

* Fri Jul 22 2016 Daniele Branchini <dbranchini@arpae.it> - 1.0-9%{dist}
- fixed tests
- fixed #37

* Thu May 19 2016 Daniele Branchini <dbranchini@arpa.emr.it> - 1.0-8%{dist}
- arki-server rewrote in python

* Thu Jan  7 2016 Daniele Branchini <dbranchini@arpa.emr.it> - 1.0-7%{dist}
- Fixed #8, #10, #14, #18, #26, #34
- Addressed #7

* Thu Jan  7 2016 Daniele Branchini <dbranchini@arpa.emr.it> - 1.0-1%{dist}
- Ported to c++11

* Tue Oct 13 2015 Daniele Branchini <dbranchini@arpa.emr.it> - 0.81-1%{dist}
- Ported to wreport 3
- Fixed argument passing, that caused use of a deallocated string

* Wed Feb  4 2015 Daniele Branchini <dbranchini@arpa.emr.it> - 0.80-3153%{dist}
- fixed arki-scan out of memory bug

* Wed Jan 21 2015 Daniele Branchini <dbranchini@arpa.emr.it> - 0.80-3146%{dist}
- fixed large query bug

* Thu Mar 20 2014 Emanuele Di Giacomo <edigiacomo@arpa.emr.it> - 0.75-2876%{dist}
- libwibble-devel dependency (--enable-wibble-standalone in configure)
- VM2 derived values in serialization

* Tue Sep 10 2013 Daniele Branchini <dbranchini@arpa.emr.it> - 0.75-2784%{dist}
- SmallFiles support
- split SummaryCache in its own file
- arkiguide.it

* Wed Jun 12 2013 Daniele Branchini <dbranchini@arpa.emr.it> - 0.74-2763%{dist}
- corretto bug nel sort dei dati

* Wed May 22 2013 Daniele Branchini <dbranchini@arpa.emr.it> - 0.74-2759%{dist}
- arki-check now can do repack and archival in a single run
- arki-check now does not do repack if a file is to be deleted
- added support for VM2 data
- arki-scan now supports bufr:- for scanning BUFR files from standard input
- ODIMH5 support moves towards a generic HDF5 support

* Wed Jan  9 2013 Emanuele Di Giacomo <edigiacomo@arpa.emr.it> - 0.73-2711%{dist}
- Rebuild to reflect upstream changes (fixed arki-xargs serialization)

* Mon Nov 26 2012 Daniele Branchini <dbranchini@arpa.emr.it> - 0.73-2677%{dist}
- Rebuild to reflect upstream changes (adding meteo-vm2)

* Tue Jul  1 2008 root <enrico@enricozini.org> - 0.4-1
- Initial build.
