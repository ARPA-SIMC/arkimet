#!/bin/sh -e

## Clean up the test environment at exit unless asked otherwise
cleanup() {
    test -z "$PAUSE" || sh
    test -z "$PRESERVE" && rm -rf "$TESTDIR"
}

# Setup the test environment and change into the test dir
setup() {
    TOP_SRCDIR=`pwd`/`dirname $0`/..
    BINDIR=`pwd`/`dirname $0`/../src

    export PATH="$BINDIR:$PATH"

    CMD=`pwd`/"$1"

    ## Set up the test environment

    export ARKI_SCAN_GRIB1=$TOP_SRCDIR/conf/scan-grib1/
    export ARKI_SCAN_GRIB2=$TOP_SRCDIR/conf/scan-grib2/
    export ARKI_FORMATTER=$TOP_SRCDIR/conf/format/
    export ARKI_REPORT=$TOP_SRCDIR/conf/report/
    export ARKI_POSTPROC=$TOP_SRCDIR/test/postproc/
    export ARKI_ALIASES=$TOP_SRCDIR/conf/match-alias.conf
    export http_proxy=

    TESTDIR="`mktemp -d`"
    cd "$TESTDIR"

    mkdir inbound
    # Put data in test inbound area
    cp -a "$TOP_SRCDIR/test/data/"* inbound/
    # Create test dataset directories
    mkdir test200
    mkdir test80
    mkdir error

    trap cleanup EXIT
}

## Call at end of the test script
endtest() {
    if [ ! -z "$PAUSE" ]
    then
        echo "Post-test inspection requested."
        echo "Exit this shell to cleanup the test environment."
        bash
    fi

    exit 0
}

importtest() {
    # Create the test datasets and import the test data

    cat <<EOT > test200/config
type=local
step=daily
filter=origin:GRIB1,200
indef=origin
postprocess=cat,echo,say
EOT

    cat <<EOT > test80/config
type=local
step=daily
filter=origin:GRIB1,80
indef=origin
EOT

    cat <<EOT > error/config
type = error
step = daily
EOT

    arki-mergeconf -o conf test200 test80 error

    arki-scan --dispatch=conf inbound/test.grib1 > /dev/null 2>&1
}

OKCOUNT=0
BADCOUNT=0
TOTCOUNT=0

runtest() {
    #echo "TEST $@"
    TOTCOUNT=`expr $TOTCOUNT + 1`
    if [ $# == 1 ]
    then
        name="$1"
    else
        name=$1
        shift
    fi
    if eval $@
    then
        echo -n '.'
        OKCOUNT=`expr $OKCOUNT + 1`
    else
        echo "Failed: $name"
        BADCOUNT=`expr $BADCOUNT + 1`
    fi
}

testtotals() {
    echo "$TOTCOUNT tests, $OKCOUNT ok, $BADCOUNT fail"

    if [ ! -z "$SERVERPAUSE" ]
    then
	    bash
    fi
}

testempty() {
    test `wc -l` = 0
}

testnonempty() {
    test `wc -l` != 0
}

testequals() {
    local BODY=`cat`
    if [ "$BODY" = "$1" ]
    then
        return 0
    else
        echo "Expected '$1', found '$BODY'" >&2
        return 1
    fi
}
