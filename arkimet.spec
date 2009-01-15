Summary: Archive for weather information
Name: arkimet
Version: 0.4
Release: 1
License: GPL
Group: Applications/Meteo
URL: http://www.arpa.emr.it/dettaglio_documento.asp?id=1172&idlivello=64
Source0: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
BuildRequires: doxygen, libdballe-devel >= 4.0.0, lua-devel >= 5.1, grib_api, sqlite-devel >= 3.0, curl-devel, geos-devel, pkgconfig, python-cherrypy
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
[ "%{buildroot}" != / ] && rm -rf "%{buildroot}"
#rm -rf $RPM_BUILD_ROOT
make install DESTDIR=$RPM_BUILD_ROOT

%clean
[ "%{buildroot}" != / ] && rm -rf "%{buildroot}"
#rm -rf $RPM_BUILD_ROOT




%files
%defattr(-,root,root,-)
%dir /etc/arkimet
/etc/arkimet/*
%dir /usr/bin
%dir /usr/share/man/man1
/usr/bin/*
%doc /usr/share/man/man1/*

%files -n arkimet-devel
%defattr(-,root,root,-)
/usr/include/arki/*
/usr/lib/libarkimet*.a
/usr/lib/libarkimet*.la
/usr/lib/libarkimet*.so
#/usr/lib/pkgconfig/libarkimet*
#/usr/share/aclocal/libarkimet*.m4

%files -n libarkimet0
%defattr(-,root,root,-)
/usr/lib/libarkimet*.so.*

%post
/sbin/ldconfig

%postun
/sbin/ldconfig


%changelog
* Tue Jul  1 2008 root <enrico@enricozini.org> - 0.4-1
- Initial build.
