from __future__ import print_function
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals
from fabric.api import local, run, sudo, cd, env, hosts, shell_env
from fabric.contrib.files import exists
import git
import re
from six.moves import shlex_quote

env.hosts = ["venti", "ventiquattro", "sette"]
env.use_ssh_config = True

_common_configure_args = [
        "--build=x86_64-redhat-linux-gnu",
        "--host=x86_64-redhat-linux-gnu",
        "--program-prefix=",
        "--prefix=/usr",
        "--exec-prefix=/usr",
        "--bindir=/usr/bin",
        "--sbindir=/usr/sbin",
        "--sysconfdir=/etc",
        "--datadir=/usr/share",
        "--includedir=/usr/include",
        "--libdir=/usr/lib64",
        "--libexecdir=/usr/libexec",
        "--localstatedir=/var",
        "--sharedstatedir=/var/lib",
        "--mandir=/usr/share/man",
        "--infodir=/usr/share/info",
        "--disable-dependency-tracking",
]
CONFIGURE_ARGS = {
    "venti": _common_configure_args + ["--enable-arpae-tests"],
    "ventiquattro": _common_configure_args + ["--enable-arpae-tests"],
    "sette": _common_configure_args,
}

_common_cxxflags = "-O2 -g -pipe -Wall -Wp,-D_FORTIFY_SOURCE=2 -fexceptions -fstack-protector-strong"\
                   " --param=ssp-buffer-size=4 -grecord-gcc-switches  -m64 -mtune=generic"
CXXFLAGS = {
    "venti": _common_cxxflags,
    "ventiquattro": _common_cxxflags + "-Werror=format-security -specs=/usr/lib/rpm/redhat/redhat-hardened-cc1",
    "sette": _common_cxxflags,
}

_common_ldflags = "-Wl,-z,relro"
LDFLAGS = {
    "venti": _common_ldflags,
    "ventiquattro": _common_ldflags + " -specs=/usr/lib/rpm/redhat/redhat-hardened-ld",
    "sette": _common_ldflags,
}

TEST_WORKAREA = {
    "venti": "/tmp",
    "ventiquattro": "/tmp",
    "sette": "/home/makerpm/testarea",
}


def cmd(*args):
    return " ".join(shlex_quote(a) for a in args)


def _test(name):
    cxxflags = CXXFLAGS[name]
    ldflags = LDFLAGS[name]

    repo = git.Repo()
    remote = repo.remote(name)
    push_url = remote.config_reader.get("url")
    remote_dir = re.sub(r"^ssh://[^/]+", "", push_url)
    configure_cmd = ["./configure"] + CONFIGURE_ARGS[name] + [
        "CFLAGS=" + cxxflags,
        "CXXFLAGS=" + cxxflags,
        "LDFLAGS=" + ldflags,
    ]
    test_workarea = TEST_WORKAREA[name]

    local(cmd("git", "push", "--force", name, "HEAD"))
    with cd(remote_dir):
        run(cmd("git", "reset", "--hard"))
        run(cmd("git", "clean", "-fx"))
        run(cmd("git", "checkout", "-B", "test_" + name, repo.head.commit.hexsha))
        run(cmd("autoreconf", "-if"))
        run(cmd(*configure_cmd))
        run(cmd("make"))
        with shell_env(TMPDIR=test_workarea):
            run(cmd("make", "check", "TEST_VERBOSE=1"))


@hosts("venti")
def test_venti():
    _test("venti")


@hosts("ventiquattro")
def test_ventiquattro():
    _test("ventiquattro")


@hosts("makerpm@sette")
def test_sette():
    _test("sette")


def test():
    test_venti()
    test_ventiquattro()
