#!/bin/bash
set -exo pipefail

image=$1

if [[ $image =~ ^centos: ]]
then
    pkgcmd="yum"
    builddep="yum-builddep"
    sed -i '/^tsflags=/d' /etc/yum.conf
    yum install -q -y epel-release
    yum install -q -y @buildsys-build
    yum install -q -y yum-utils
    yum install -q -y yum-plugin-copr
    yum install -q -y git
    yum copr enable -q -y simc/stable
elif [[ $image =~ ^fedora: ]]
then
    pkgcmd="dnf"
    builddep="dnf builddep"
    sed -i '/^tsflags=/d' /etc/dnf/dnf.conf
    dnf install -q -y @buildsys-build
    dnf install -q -y 'dnf-command(builddep)'
    dnf install -q -y git
    dnf copr enable -q -y simc/stable
fi

$builddep -y fedora/SPECS/arkimet.spec

if [[ $image =~ ^fedora: || $image =~ ^centos: ]]
then
    pkgname=arkimet-master
    mkdir -p ~/rpmbuild/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
    cp fedora/SPECS/arkimet.spec ~/rpmbuild/SPECS/arkimet.spec
    cp fedora/SOURCES/* ~/rpmbuild/SOURCES/
    git archive --prefix=$pkgname/ --format=tar HEAD | gzip -c > ~/rpmbuild/SOURCES/$pkgname.tar.gz
    rpmbuild -ba --define "srcarchivename $pkgname" ~/rpmbuild/SPECS/arkimet.spec
    find ~/rpmbuild/{RPMS,SRPMS}/ -name "*rpm" -exec cp -v {} . \;
    # TODO upload ${pkgname}*.rpm to github release on deploy stage
else
    autoreconf -ifv
    ./configure
    make check TEST_VERBOSE=1
fi
