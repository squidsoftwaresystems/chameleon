project('chameleon', 'cpp')

subdir('cpp/src/schedule')

incdir = include_directories('cpp/include/schedule')

py = import('python').find_installation(pure: false)

py.extension_module(
    'schedule',
    sources,
    install: true,
    include_directories : incdir
)
