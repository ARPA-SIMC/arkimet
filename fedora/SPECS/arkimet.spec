Summary: Archive for weather information
Name: arkimet
Version: 0.73
Release: 2748%{dist}
License: GPL
Group: Applications/Meteo
URL: http://www.arpa.emr.it/sim/?arkimet‎
Source0: %{name}-%{version}.tar.gz
Source1: %{name}.init
Source2: %{name}.default
Patch0: %{name}.wibble-hidden-option.patch
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
BuildRequires: doxygen, libdballe-devel >= 5.19, lua-devel >= 5.1, grib_api-devel, sqlite-devel >= 3.6, curl-devel, geos-devel, pkgconfig, python-cherrypy, readline-devel, lzo-devel, libwreport-devel >= 2.0, flex, bison, meteo-vm2-devel, hdf5-devel
Requires: python-cherrypy, hdf5, meteo-vm2, grib_api-1.10.0
Requires(preun): /sbin/chkconfig, /sbin/service
Requires(post): /sbin/chkconfig, /sbin/service

%{!?python_sitelib: %define python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}
%{!?python_sitearch: %define python_sitearch %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib(1)")}
%{!?python_siteinc: %define python_siteinc %(%{__python} -c "from distutils.sysconfig import get_python_inc; print get_python_inc()")}

Requires: python >= 2.5

%description
 Description to be written.

%package  -n arkimet-devel
Summary:  Archive for weather information (development library)
Group:    Applications/Meteo

%description -n arkimet-devel
 Description to be written.

%prep
%setup -q
%patch0 -p0

%build
%configure
make
#make check

%install
[ "%{buildroot}" != / ] && rm -rf %{buildroot}
%makeinstall

install -D -m0755 %{SOURCE1} %{buildroot}%{_sysconfdir}/rc.d/init.d/%{name}
install -bD -m0755 %{SOURCE2} %{buildroot}%{_sysconfdir}/default/arki-server


%clean
[ "%{buildroot}" != / ] && rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%dir %{_sysconfdir}/arkimet
%config(noreplace) %{_sysconfdir}/arkimet/match-alias.conf
%{_sysconfdir}/arkimet/*
%{_bindir}/*
%{_sysconfdir}/rc.d/init.d/%{name}
%config(noreplace) %{_sysconfdir}/default/arki-server
%doc %{_mandir}/man1/*
%doc README TODO NEWS
%doc doc/*


%files -n arkimet-devel
%defattr(-,root,root,-)
%{_libdir}/libarkimet*.a
%{_libdir}/libarkimet*.la
%dir %{_includedir}/arki
%{_includedir}/arki/*


%pre
/sbin/service %{name} stop >/dev/null 2>&1

%post
/sbin/ldconfig
/sbin/chkconfig --add %{name}

%preun

%postun
/sbin/ldconfig
if [ "$1" = "0" ]; then
  # non e' un upgrade, e' una disinstallazione definitiva
  /sbin/chkconfig --del %{name}
fi


%changelog
* Wed Jan  9 2013 Emanuele Di Giacomo <edigiacomo@arpa.emr.it> - 0.73-2711%{dist}
- Rebuild to reflect upstream changes (fixed arki-xargs serialization)

* Mon Nov 26 2012 Daniele Branchini <dbranchini@arpa.emr.it> - 0.73-2677%{dist}
- Rebuild to reflect upstream changes (adding meteo-vm2)

* Thu Apr 19 2012 Daniele Branchini <dbranchini@arkitest> - 0.71-2505%{dist}
- Rebuild to reflect upstream changes

* Thu Feb 23 2012 Daniele Branchini <dbranchini@arkitest> - 0.69-2497%{dist}
- Rebuild to reflect upstream changes

* Mon Sep 19 2011 Daniele Branchini <dbranchini@linus> - 0.64-2451%{dist}
- Rebuild to reflect upstream changes

* Fri Aug 12 2011 Daniele Branchini <dbranchini@linus> - 0.63-2431%{dist}
- Effettivo supporto per i 32bit

* Wed Jul 13 2011 Daniele Branchini <dbranchini@linus> - 0.62-2402%{dist}
- corretto initscript

* Mon Jul 11 2011 Daniele Branchini <dbranchini@linus> - 0.62-2401%{dist}
- Aggiunto initscript

* Thu Oct 14 2010 Daniele Branchini <dbranchini@pigna> - 0.46-2163%{dist}
- Added radarlib support

* Mon Jul 12 2010 Daniele Branchini <dbranchini@linus> - 0.44-2067%{dist}
- Corretta dipendenza da libarkimet0 (obsoleto)

* Thu Jul  8 2010 Daniele Branchini <dbranchini@carenza.metarpa> - 0.44-2066%{dist}
- Rebuild to reflect upstream changes, removed libarkimet0

* Thu Sep 17 2009 root <root@localhost.localdomain> - 0.27-1418
- Rebuild to reflect upstream changes.

* Tue Sep  8 2009 root <root@localhost.localdomain> - 0.26.1-1409
- added some documentation files

* Mon Sep  7 2009 Daniele Branchini <dbranchini@carenza.metarpa> - 0.26.1-1
- Rebuild to reflect upstream changes.

* Wed Aug 26 2009 Daniele Branchini <dbranchini@carenza.metarpa> - 0.25-1
- Rebuild to reflect upstream changes.

* Tue Jul  1 2008 root <enrico@enricozini.org> - 0.4-1
- Initial build.
