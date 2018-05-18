#!/bin/bash
# Usage: copr-build-commit.sh [COMMIT]
#
# Build SRPM from commit COMMIT (default: HEAD) and upload it to Copr.
set -e
commit="${1:-HEAD}"
name="$(rpmspec -q --qf '%{name}-%{version}-%{release}\n' fedora/SPECS/arkimet.spec | head -n1)"
cp fedora/SOURCES/* ~/rpmbuild/SOURCES/
git archive --prefix=$name/ --format=tar $commit  | gzip -c > ~/rpmbuild/SOURCES/$name.tar.gz
rpmbuild --quiet -bs fedora/SPECS/arkimet.spec
copr-cli build --nowait simc/stable ~/rpmbuild/SRPMS/$name.src.rpm
