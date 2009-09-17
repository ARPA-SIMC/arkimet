Summary: Archive for weather information
Name: arkimet
Version: 0.26.1
Release: 1409
License: GPL
Group: Applications/Meteo
URL: http://www.arpa.emr.it/dettaglio_documento.asp?id=1172&idlivello=64
Source0: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
BuildRequires: doxygen, libdballe-devel >= 4.0.0, lua-devel >= 5.1, grib_api, sqlite-devel >= 3.0, curl-devel, geos-devel, pkgconfig, python-cherrypy, readline-devel
Requires: python-cherrypy

 
%description
 Description to be written.

%package  -n arkimet-devel
Summary:  Archive for weather information (development library)
Group:    Applications/Meteo
Requires: lib%{name}0 = %{?epoch:%epoch:}%{version}-%{release}

%description -n arkimet-devel
 Description to be written.


%package  -n libarkimet0
Summary:  Archive for weather information (runtime library)
Group:    Applications/Meteo

%description -n libarkimet0
 Description to be written.

%prep
%setup -q

%build
%configure
make
#make check

%install
[ "%{buildroot}" != / ] && rm -rf %{buildroot}
%makeinstall

%clean
[ "%{buildroot}" != / ] && rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%dir %{_sysconfdir}/arkimet
%{_sysconfdir}/arkimet/*
%{_bindir}/*
%doc %{_mandir}/man1/*
%doc README
%doc MATCHER
%doc DATASET

%files -n arkimet-devel
%defattr(-,root,root,-)
%{_includedir}/arki/*
%{_libdir}/libarkimet*.a
%{_libdir}/libarkimet*.la
%{_libdir}/libarkimet*.so
#/usr/lib/pkgconfig/libarkimet*
#/usr/share/aclocal/libarkimet*.m4

%files -n libarkimet0
%defattr(-,root,root,-)
%{_libdir}/libarkimet*.so.*

%post
/sbin/ldconfig

%postun
/sbin/ldconfig

%changelog
* Tue Sep  8 2009 root <root@localhost.localdomain> - 0.26.1-1409
- added some documentation files

* Mon Sep  7 2009 Daniele Branchini <dbranchini@carenza.metarpa> - 0.26.1-1
- Rebuild to reflect upstream changes.

* Wed Aug 26 2009 Daniele Branchini <dbranchini@carenza.metarpa> - 0.25-1
- Rebuild to reflect upstream changes.

* Tue Jul  1 2008 root <enrico@enricozini.org> - 0.4-1
- Initial build.
