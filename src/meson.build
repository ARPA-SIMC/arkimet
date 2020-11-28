scripts = [
    ['arki-query', 'Query the datasets in the given config file for data matching the given expression, and output the matching metadata.'],
    ['arki-scan', 'Read messages from files and datasets then outputs it or imports it into arkimet datasets.'],
    ['arki-check', 'Perform maintenance on arkimet datasets.'],
    ['arki-mergeconf', 'Build a single Arkimet configuration file merging configurations from multiple sources.'],
    ['arki-dump', 'Read data from the given input file (or stdin), and dump the in human readable format on stdout.'],
    ['arki-xargs', 'For every item of data read from standard input, save it on a temporary file and run "command [initial-arguments] filename" on it.'],
    ['arki-server', 'Arkimet server for remote dataset access.'],
    ['arki-testtar', 'Build a .tar version of a dataset for testing, without storing the actual data.'],
    ['arki-web-proxy', 'Dispatching proxy for arki-servers.'],
]

if dballe_dep.found()
    scripts += [['arki-bufr-prepare', 'Read BUFR messages, and encode each subsection in a separate message.']]
endif

foreach script: scripts
    install_data(script[0], install_dir: get_option('bindir'))
    custom_target(script[0] + '.1',
                  input: [script[0], '../doc' / 'mhooks-' + script[0]],
                  output: script[0] + '.1',
                  command: [run_local, help2man, '--include', '@INPUT1@', '--name=' + script[1], '--section=1', '--no-info', '--version-string=' + meson.project_version(), '@INPUT0@'],
                  capture: true,
                  install: true,
                  install_dir: get_option('mandir') / 'man1')
endforeach

