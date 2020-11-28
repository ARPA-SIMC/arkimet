scripts = ['arki-query', 'arki-scan', 'arki-check', 'arki-mergeconf', 'arki-dump', 'arki-xargs', 'arki-server', 'arki-testtar', 'arki-web-proxy']

if dballe_dep.found()
    scripts += ['arki-bufr-prepare']
endif

foreach script: scripts
    install_data(script, install_dir: get_option('bindir'))
endforeach
