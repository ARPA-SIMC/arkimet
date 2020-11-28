libarkimet_sources += files(
    'lexer.cc',
    'parser.cc',
)

libarkimet_sources += custom_target('reftime_lexer',
                        output: ['reftime-lex.h', 'reftime-lex.cc'],
                        input: ['reftime-lex.ll'],
                        command: [flex, '--outfile=@OUTDIR@' / '@BASENAME@.cc', '--header-file=@OUTDIR@' / '@BASENAME@.h', '@INPUT@'])

libarkimet_sources += custom_target('reftime_parser',
                        output: ['reftime-parse.hh', 'reftime-parse.cc'],
                        input: ['reftime-parse.yy'],
                        command: [bison, '--output=@OUTDIR@' / '@BASENAME@.cc', '@INPUT@'])
