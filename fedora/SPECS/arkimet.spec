# Note: define srcarchivename in Travis build only.
%{!?srcarchivename: %global srcarchivename %{name}-%{version}-%{release}}

Summary: Archive for weather information
Name: arkimet
Version: 1.44
Release: 1
License: GPL
Group: Applications/Meteo
URL: https://github.com/arpa-simc/%{name}
Source0: https://github.com/arpa-simc/%{name}/archive/v%{version}-%{release}.tar.gz#/%{srcarchivename}.tar.gz
Source1: https://github.com/arpa-simc/%{name}/raw/v%{version}-%{release}/fedora/SOURCES/%{name}.service
Source2: https://github.com/arpa-simc/%{name}/raw/v%{version}-%{release}/fedora/SOURCES/%{name}.sysconfig
Source3: https://github.com/arpa-simc/%{name}/raw/v%{version}-%{release}/fedora/SOURCES/%{name}-logrotate.conf

# On fedora, we don't need systemd to build. But we do on centos.
# https://fedoraproject.org/wiki/Packaging:Systemd#Filesystem_locations
BuildRequires: systemd

# Python 3 package names
%if 0%{?rhel} == 7
%define python3_vers python36
# to have python 3.6 interpreter
BuildRequires: python3-rpm-macros >= 3-23
%else
%define python3_vers python3
%endif

BuildRequires: autoconf
BuildRequires: gcc-c++
BuildRequires: libtool
BuildRequires: doxygen
BuildRequires: pkgconfig(libdballe) >= 9.0
BuildRequires: lua-devel >= 5.1
BuildRequires: eccodes-devel
BuildRequires: eccodes-simc
BuildRequires: sqlite-devel >= 3.6
BuildRequires: curl
BuildRequires: curl-devel
BuildRequires: geos-devel
BuildRequires: popt-devel
BuildRequires: hdf5-devel
BuildRequires: help2man
BuildRequires: pkgconfig
BuildRequires: readline-devel
BuildRequires: lzo-devel
BuildRequires: libwreport-devel >= 3.0
BuildRequires: flex
BuildRequires: bison
BuildRequires: meteo-vm2-devel >= 0.12
BuildRequires: %{python3_vers}
BuildRequires: %{python3_vers}-devel
BuildRequires: %{python3_vers}-werkzeug
BuildRequires: %{python3_vers}-setproctitle
BuildRequires: %{python3_vers}-jinja2
BuildRequires: %{python3_vers}-requests
BuildRequires: %{python3_vers}-wreport3
BuildRequires: %{python3_vers}-dballe >= 9.0
BuildRequires: %{python3_vers}-netcdf4
BuildRequires: %{python3_vers}-pillow
%if ! 0%{?el7}
BuildRequires: %{python3_vers}-h5py
BuildRequires: %{python3_vers}-sphinx
%else
BuildRequires: h5py
%endif
BuildRequires: libzip-devel
BuildRequires: libarchive-devel
BuildRequires: bzip2-devel
BuildRequires: %{python3_vers}-shapely

Requires: meteo-vm2 >= 0.12
Requires: eccodes
Requires: %{python3_vers}
Requires: %{python3_vers}-werkzeug
Requires: %{python3_vers}-setproctitle
Requires: %{python3_vers}-dballe >= 9.0
Requires: %{python3_vers}-netcdf4
Requires: %{python3_vers}-pillow
Requires: %{python3_vers}-shapely
Requires: libdballe9
Requires: systemd
%if ! 0%{?el7}
Requires: %{python3_vers}-h5py
%else
Requires: h5py
%endif

%{!?python3_sitelib: %define python3_sitelib %(%{__python3} -c "import sysconfig; print(sysconfig.get_path('purelib'))")}
%{!?python3_sitearch: %define python3_sitearch %(%{__python3} -c "import sysconfig; print(sysconfig.get_path('platlib'))")}

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
Requires: libdballe-devel >= 9.0
Requires: eccodes-devel
Requires: libwreport-devel
Requires: %{python3_vers}-devel
Requires: meteo-vm2-devel
Requires: sqlite-devel
Requires: curl-devel
Requires: lzo-devel
Requires: libarchive-devel
Requires: libzip-devel
Requires: geos-devel
Requires: readline-devel
Requires: bzip2-devel

%description -n arkimet-devel
 Arkimet developement library

%prep
%setup -q -n %{srcarchivename}
sh autogen.sh

%build

# CentOS 7 known limitations
# - disabled syscall splice()
# - disabled nosetests that were hanging (see #217)
# - disabled netcdf v5 support (see #243)
# - disabled doc building for issues with sphinx

# enabling arpae tests on almost all builds
%{?fedora:%define arpae_tests 1}
%{?rhel:%define arpae_tests 1}

%if 0%{?arpae_tests}
echo 'Enabling ARPAE tests'
source %{_sysconfdir}/profile.d/eccodes-simc.sh
%configure --enable-arpae-tests %{?el7:--disable-docs --disable-splice}
%else
%configure %{?el7:--disable-docs --disable-splice}
%endif
make


%install
[ "%{buildroot}" != / ] && rm -rf %{buildroot}
%makeinstall

install -D -m0644 %{SOURCE1} %{buildroot}%{_unitdir}/%{name}.service
install -bD -m0644 %{SOURCE2} %{buildroot}%{_sysconfdir}/sysconfig/%{name}
install -D -m 0644 -p %{SOURCE3} %{buildroot}%{_sysconfdir}/logrotate.d/%{name}

%check

%if 0%{?arpae_tests}
echo 'Enabling ARPAE tests'
source %{_sysconfdir}/profile.d/eccodes-simc.sh
%endif


%if 0%{?el7}
# See https://github.com/ARPA-SIMC/arkimet/issues/217
make check ISSUE217=1
%else
make check
%endif


%clean
[ "%{buildroot}" != / ] && rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%dir %{_sysconfdir}/arkimet
%config(noreplace) %{_sysconfdir}/arkimet/match-alias.conf
%{_sysconfdir}/arkimet/bbox/*
%{_sysconfdir}/arkimet/format/*
%{_sysconfdir}/arkimet/qmacro/*
%{_sysconfdir}/arkimet/scan/*
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
if [ "$1" = "1" ]; then
    # it's an initial installation, not an upgrade
    /usr/bin/getent passwd arkimet >/dev/null \
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
* Tue May 31 2022 Emanuele Di Giacomo <edigiacomo@arpae.it> - 1.44-1
- Only allow GET in dataset-specific /query if style=postprocess (#289)

* Tue May 31 2022 Emanuele Di Giacomo <edigiacomo@arpae.it> - 1.43-1
- Only allow GET in /query if style=postprocess (#289)

* Tue May 31 2022 Emanuele Di Giacomo <edigiacomo@arpae.it> - 1.42-3
- Fixed again changelog

* Tue May 31 2022 Emanuele Di Giacomo <edigiacomo@arpae.it> - 1.42-2
- Fixed changelog

* Tue May 31 2022 Emanuele Di Giacomo <edigiacomo@arpae.it> - 1.42-1
- Restored HTTP GET for /query (#289)

* Tue Mar 15 2022 Emanuele Di Giacomo <edigiacomo@arpae.it> - 1.41-1
- Faster deletion of data from iseg datasets (#286)

* Tue Feb  8 2022 Daniele Branchini <dbranchini@arpae.it> - 1.40-1
- Added documentation of metadata types (#280)
- Fixed arki-web-proxy (#281)
- Added initial support for JPEG files (#277)
- Added default timeout of 15m for ARKI_IO_TIMEOUT (#270)
- Removed vestigial link to non-existing "Perform a query" form in arki-server (#144)
- arki-server now gives HTTP error 405 "Method not allowed" when appropriate (#144)
- Improved rounding in ODIM scanning (#283)

* Tue Nov 23 2021 Daniele Branchini <dbranchini@arpae.it> - 1.39-1
- In arki-check output, show the amount of data that would be freed by a repack
  (#187)
- Fixed inconsistent handling of empty directories as dir segments (#279)

* Fri Oct 22 2021 Daniele Branchini <dbranchini@arpae.it> - 1.38-2
- Refactored merged dataset reading test (#276)

* Wed Oct 20 2021 Daniele Branchini <dbranchini@arpae.it> - 1.38-1
- Dismissed `type=ondisk2` dataset support (#275)
- Implemented alias management in arki-web-proxy (#272)
- When a config file is found inside a directory, as if it were a dataset, but
  has type=remote, its `path` value does not get overridden with the full path
  to the directory (#274)

* Mon Oct 18 2021 Daniele Branchini <dbranchini@arpae.it> - 1.37-1
- Log Python scanner exceptions as warnings, since processing proceeds with a
  base set of metadata (#273)

* Fri Aug 27 2021 Emanuele Di Giacomo <edigiacomo@arpae.it> - 1.36-1
- Automatic parsing for config files and directories, disable fallback to
  arkimet format (#265)
- Fixed postprocessor hanging (#269)

* Mon May 17 2021 Daniele Branchini <dbranchini@arpae.it> - 1.35-1
- Added support for libnetcdf.so.15
- Minor fixes in dependency checking and tests (#266, #267)

* Wed Apr 28 2021 Daniele Branchini <dbranchini@arpae.it> - 1.34-1
- Fixed satellite grib2 import (#264)
- Implemented server-side timeout for arki-queries (#252)
- Removed grib_api package support

* Tue Jan 26 2021 Daniele Branchini <dbranchini@arpae.it> - 1.33-1
- Fixed arki-bufr-prepare `--usn` (#260)
- Improved doc (#261, #235)

* Fri Jan 15 2021 Daniele Branchini <dbranchini@arpae.it> - 1.32-1
- Added missing nc.py install (#257)

* Thu Jan 14 2021 Daniele Branchini <dbranchini@arpae.it> - 1.31-1
- Fixed a serious issue in encoding/decoding GRIB1 timeranges (#256)
- Added `eatmydata` dataset config option documentation (#233)

* Wed Jan 13 2021 Daniele Branchini <dbranchini@arpae.it> - 1.30-1
- Added initial NetCDF support (#40)
- Metadata refactoring to address memory issues (#242, #245)
- Implemented `eatmydata: yes` in dataset conf (#233)
- Documented supported dataset configuration options (#143)
- VM2 data: implemented a normalisation function, fixed smallfile issue (#237)
- Fixed segfault in python test suite (#254)
- Implemented `arki.Matcher.merge(matcher)` (#255)
- Implemented `arki.Matcher.update(matcher)`
- Refactored ValueBag/Values with a simple structure (#248)
- Break circular dependency by decoupling Session and Pool (#250)
- Fixed segfault when query data is appended to a file from a iseq dataset (#244)

* Fri Aug 14 2020 Emanuele Di Giacomo <edigiacomo@arpae.it> - 1.29-1
- Fixed error reading offline archives (#232)
- Allow to create an arkimet session with `force_dir_segments=True` to always
  store each data in a separate file, to help arkimaps prototyping.

* Thu Jul 09 2020 Emanuele Di Giacomo <edigiacomo@arpae.it> - 1.28-1
- Added `format_metadata.rst` to Python HOWTOs (#230)
- Python: added `arkimet.Summary.read_binary` (#230)
- Python: added `arkimet.Summary.read_yaml` (#230)
- Python: added `arkimet.Summary.read_json` (#230)
- Python: added `annotate: bool = False` to `arkimet.Summary.write` (#230)
- Python: added `annotate: bool = False` to `arkimet.Summary.write_short` (#230)
- Python: implemented `arkimet.Metadata.write` for `yaml` and `json` formats (#230)
- Python: added `annotate: bool = False` to `arkimet.Metadata.write` (#230)
- Python: added `arkimet.Metadata.read_yaml` (#230)
- Python: added `arkimet.Metadata.read_json` (#230)

* Wed Jun 10 2020 Daniele Branchini <dbranchini@arpae.it> - 1.27-1
- reftime match expressions using repeating intervals (like `reftime:=yesterday
  %3h`) now have a new `@hh:mm:ss` syntax to explicitly reference the starting
  point of the repetitions. For example, `arki-dump --query 'reftime:=2020-01-01 20:30%8h'`
  yields `reftime:>=2020-01-01 20:30:00,<2020-01-01 20:31:00,@04:30:00%8h`,
  meaning a match in that interval and at 20:30 every 8 hours.
  (side effect of fixing #206)
- Added Python bindings for arki.dataset.Session
  (side effect of fixing #206)
- Added Python bindings for arkimet.dataset.Dataset
  (side effect of fixing #206)
- Python code should now instantiate datasets and matchers using Session; the
  old way still works, and raises `DeprecationWarning`s
- Python function `arkimet.make_qmacro_dataset()` deprecated in favour of
  `arkimet.dataset.Session.querymacro()`
- Python function `arkimet.make_merged_dataset()` deprecated in favour of
  `arkimet.Session.merged()`
- Python function `arkimet.get_alias_database()` deprecated in favour of
  `arkimet.dataset.Session.get_alias_database()`
- Python function `arkimet.expand_query()` deprecated in favour of
  `arki.dataset.Session().expand_query()`
- Correctly query multiple remote servers with different but compatible alias
  configurations, in merged and querymacro datasets (#206)

* Mon Jun  1 2020 Daniele Branchini <dbranchini@arpae.it> - 1.26-1
- Fixed use of geos compile-time version detection (#225)
- Removed make install depending on make check (#218)
- Fix ODIM scanner for volumes with missing 'radhoriz' parameter (#227)

* Fri May  8 2020 Daniele Branchini <dbranchini@arpae.it> - 1.25-1
- Fixes for geos ≥ 3.8.0 (#224)

* Thu Mar 26 2020 Daniele Branchini <dbranchini@arpae.it> - 1.24-1
- Fixed return value of arki-query (#221)

* Mon Mar  2 2020 Daniele Branchini <dbranchini@arpae.it> - 1.23-1
- Added `arki-query --progress`
- Restored `processing..` message on `arki-query --verbose` (#216)
- Added a periodic callback during queries, used to check for SIGINT and raising KeyboardError (#210)
- Removed spurious returns in `Summary.write` (#214)
- Fixed database queries with empty reftime values (#215)

* Mon Feb 17 2020 Daniele Branchini <dbranchini@arpae.it> - 1.22-1
- Keep a Sesssion structure which tracks and reuses Segment objects, and as a consequence, file descriptors (#213)
- Fixed typo in GRIB2 scanner and related GRIB2 test

* Thu Feb 13 2020 Daniele Branchini <dbranchini@arpae.it> - 1.21-1
- Fixed sorted data queries in python bindings (#212)

* Mon Feb 10 2020 Emanuele Di Giacomo <edigiacomo@arpae.it> - 1.20-1
- Fixed hanging of arki-query when using postprocessor (#209)

* Tue Jan 28 2020 Emanuele Di Giacomo <edigiacomo@arpae.it> - 1.19-1
- Fixed level and timerange formatter for GRIB1 and GRIB2 (#207)

* Wed Jan  8 2020 Emanuele Di Giacomo <edigiacomo@arpae.it> - 1.18-1
- Ported documentation to sphinx
- Fixed arki-server
- arki-web-proxy command

* Mon Dec  9 2019 Daniele Branchini <dbranchini@arpae.it> - 1.17-1
- Greatly enlarged Python API
- New HTTP API
- Redesigned and unified all the documentation (see https://arpa-simc.github.io/arkimet/)
- Ported command line scripts to python
- Removed unused gridspace querymacro
- Removed unused --targetfile option
- Removed unused --report option
- Ported all Lua infrastructure and scripts to Python
- Ported to new dballe 8 API
- Cleaned up tests and autoconf checks
- Use POSIX return values from command line tools (see https://www.freebsd.org/cgi/man.cgi?query=sysexits)
- Work when sendfile is not available
- ondisk2 datasets are deprecated from now on in favour of iseg, but still fully supported
- sphinx-based documentation for python bindings
- Fixed order of iteration / overriding options of scanners (#177)
- Fixed --annotate when some timedef keys are missing (#202)
- Do not stop at reporting the first 'dirty' cause found in a segment (#187)

* Tue Apr 16 2019 Daniele Branchini <dbranchini@arpae.it> - 1.16-1
- moving to python 3.6 on Centos7
- fixed errors in zip utils (#173)

* Thu Apr  4 2019 Daniele Branchini <dbranchini@arpae.it> - 1.15-1
- Fixed bug on dataset compression (--zip option, see #170)

* Wed Feb 27 2019 Daniele Branchini <dbranchini@arpae.it> - 1.14-1
- Ported to new dballe 8 API

* Mon Jan 21 2019 Daniele Branchini <dbranchini@arpae.it> - 1.13-1
- Fail a match on a metadata which does not have the data to match (#166)

* Fri Dec 21 2018 Daniele Branchini <dbranchini@arpae.it> - 1.12-1
- Fixed copyok/copyko empty file creation (#155)
- Filter input from stdin (#164, #159)

* Wed Dec 19 2018 Daniele Branchini <dbranchini@arpae.it> - 1.11-1
- Fixed tar option (#162)
- Restored odim sequence file behaviour (#163)

* Fri Dec 14 2018 Daniele Branchini <dbranchini@arpae.it> - 1.10-1
- Check and repack are able to fix an interrupted append to a dir dataset (#156)
- Fixed writing VM2 as inline metadata (#160)
    
* Wed Dec 5 2018 Daniele Branchini <dbranchini@arpae.it> - 1.9-1
- Inherit grib definition from environment variables
- Updated arkiguide (#157)

* Mon Nov 26 2018 Daniele Branchini <dbranchini@arpae.it> - 1.8-3
- Fixed #138

* Wed Oct 17 2018 Emanuele Di Giacomo <edigiacomo@arpae.it> - 1.8-2
- Compiled against eccodes 2.8.0-3

* Thu Oct 11 2018 Daniele Branchini <dbranchini@arpae.it> - 1.8-1
- Implemented arki-scan --flush-threshold, with a default of 128Mi (#151)

* Thu Sep 20 2018 Daniele Branchini <dbranchini@arpae.it> - 1.7-7
- Make arki-check --delete only work if --fix is present (#147)
- Fixed other dependencies for arkimet-devel package
- Updated wobble    

* Tue Sep 18 2018 Daniele Branchini <dbranchini@arpae.it> - 1.7-6
- Fixed dependencies for arkimet-devel package

* Tue Jul 17 2018 Emanuele Di Giacomo <edigiacomo@arpae.it> - 1.7-5
- Fix for internal f20/f24 grib_api arpae tests
- Updated alias for "cloud liquid water content"
- Updated dballe dependency
- Changed Requires and BuildRequires style (one package per line)

* Thu Jun 7 2018 Daniele Branchini <dbranchini@arpae.it> - 1.7-4
- arki-mergeconf can ignore error and duplicates datasets
- patch for f20 compiler bug (#142)

* Fri May 18 2018 Daniele Branchini <dbranchini@arpae.it> - 1.7-3
- skipped largefile tests for tmpfs (#140)

* Mon May 7 2018 Daniele Branchini <dbranchini@arpae.it> - 1.7-1
- implemented arki-check --remove-old (#94)
- implemented --archive=[tar|tar.gz|tar.xz|zip] in arki-query and arki-scan (#131) (#95)
- implemented arki-check --compress
- implemented arki-check --zip
- implemented arki-query/arki-scan --stdin
- removed unused files (#133)
- timerange metadata for generic BUFR (#125)
- xargs unlink tmpfile if exists (#134)
    
* Thu Feb 15 2018 Daniele Branchini <dbranchini@arpae.it> - 1.6-3
- fixed #124

* Wed Feb 14 2018 Daniele Branchini <dbranchini@arpae.it> - 1.6-2
- fixed #120, addressed #123

* Tue Jan 23 2018 Daniele Branchini <dbranchini@arpae.it> - 1.6-1
- fixed #76, #107, #110, #115, #116, #117, #118, #119, #121, #122

* Mon Nov 27 2017 Daniele Branchini <dbranchini@arpae.it> - 1.5-2
- fixed #103, #108, #109, #111, #113

* Tue Oct 17 2017 Daniele Branchini <dbranchini@arpae.it> - 1.4-3
- fixed #104
- added local arki-server for test suite
- aliases for COSMO-5M

* Thu Sep 14 2017 Daniele Branchini <dbranchini@arpae.it> - 1.4-2
- fixed error testing arkimet user
- fixed #102

* Mon Sep 4 2017 Daniele Branchini <dbranchini@arpae.it> - 1.4-1
- Fixed #65, #73, #75, #79, #80, #87, #93, #100

* Mon Jun 26 2017 Daniele Branchini <dbranchini@arpae.it> - 1.3-2
- Added logrotate conf (#98)

* Thu Jun 22 2017 Daniele Branchini <dbranchini@arpae.it> - 1.3-1
- Fixed issues in arki-check (#90, #91)
- Use exception.code in response (if possible)
- Test code cleanup
    
* Wed Jun 7 2017 Daniele Branchini <dbranchini@arpae.it> - 1.2-1
- Added documentation for dataset formats and check/fix/repack procedures (#88)
- Added verbose reporting to vacuum operations, and some test refactoring (#89)
- HTTP authentication method (any) and optional .netrc (#81)

* Thu Apr 6 2017 Daniele Branchini <dbranchini@arpae.it> - 1.1-2
- fixed #83

* Tue Feb 14 2017 Daniele Branchini <dbranchini@arpae.it> - 1.1-1
- New dataset format: one index per segment (#60)

* Tue Jan 3 2017 Daniele Branchini <dbranchini@arpae.it> - 1.0-18
- fixed #63, #64, #66, #68, #70

* Thu Dec 15 2016 Daniele Branchini <dbranchini@arpae.it> - 1.0-17
- fixed #9, #61, #62

* Tue Oct 18 2016 Daniele Branchini <dbranchini@arpae.it> - 1.0-16
- fixed #55, #56, #57, #43, #48
- fixed #54 (implemented --copyok/copyko)

* Mon Sep 26 2016 Daniele Branchini <dbranchini@arpae.it> - 1.0-15
- fixed #53
- fixed #51

* Tue Sep 20 2016 Daniele Branchini <dbranchini@arpae.it> - 1.0-14
- fixed dependencies
- commented out directive in systemd script that caused fedora20 server to hang

* Tue Sep 13 2016 Daniele Branchini <dbranchini@arpae.it> - 1.0-13
- fixed #49
- implemented output format `arki-server --journald`
- implemented output format `arki-server --perflog`

* Wed Sep 7 2016 Daniele Branchini <dbranchini@arpae.it> - 1.0-12
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

* Wed Aug 10 2016 Davide Cesari <dcesari@arpae.it> - 1.0-10
- switch to systemd

* Fri Jul 22 2016 Daniele Branchini <dbranchini@arpae.it> - 1.0-9
- fixed tests
- fixed #37

* Thu May 19 2016 Daniele Branchini <dbranchini@arpa.emr.it> - 1.0-8
- arki-server rewrote in python

* Thu Jan  7 2016 Daniele Branchini <dbranchini@arpa.emr.it> - 1.0-7
- Fixed #8, #10, #14, #18, #26, #34
- Addressed #7

* Thu Jan  7 2016 Daniele Branchini <dbranchini@arpa.emr.it> - 1.0-1
- Ported to c++11

* Tue Oct 13 2015 Daniele Branchini <dbranchini@arpa.emr.it> - 0.81-1
- Ported to wreport 3
- Fixed argument passing, that caused use of a deallocated string

* Wed Feb  4 2015 Daniele Branchini <dbranchini@arpa.emr.it> - 0.80-3153
- fixed arki-scan out of memory bug

* Wed Jan 21 2015 Daniele Branchini <dbranchini@arpa.emr.it> - 0.80-3146
- fixed large query bug

* Thu Mar 20 2014 Emanuele Di Giacomo <edigiacomo@arpa.emr.it> - 0.75-2876
- libwibble-devel dependency (--enable-wibble-standalone in configure)
- VM2 derived values in serialization

* Tue Sep 10 2013 Daniele Branchini <dbranchini@arpa.emr.it> - 0.75-2784
- SmallFiles support
- split SummaryCache in its own file
- arkiguide.it

* Wed Jun 12 2013 Daniele Branchini <dbranchini@arpa.emr.it> - 0.74-2763
- corretto bug nel sort dei dati

* Wed May 22 2013 Daniele Branchini <dbranchini@arpa.emr.it> - 0.74-2759
- arki-check now can do repack and archival in a single run
- arki-check now does not do repack if a file is to be deleted
- added support for VM2 data
- arki-scan now supports bufr:- for scanning BUFR files from standard input
- ODIMH5 support moves towards a generic HDF5 support

* Wed Jan  9 2013 Emanuele Di Giacomo <edigiacomo@arpa.emr.it> - 0.73-2711
- Rebuild to reflect upstream changes (fixed arki-xargs serialization)

* Mon Nov 26 2012 Daniele Branchini <dbranchini@arpa.emr.it> - 0.73-2677
- Rebuild to reflect upstream changes (adding meteo-vm2)

* Tue Jul  1 2008 root <enrico@enricozini.org> - 0.4-1
- Initial build.
