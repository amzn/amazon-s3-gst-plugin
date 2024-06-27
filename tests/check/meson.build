element_tests = [
  's3sink.c',
  'uploaderpartcache.cpp',
  's3sink-cached.cpp'
]

element_tests_includes = include_directories('.')

foreach test_file : element_tests
  test_name = test_file.split('.').get(0).underscorify()

  exe = executable(test_name, test_file,
    include_directories : [configinc, element_tests_includes],
    dependencies : [s3elements_dep, gst_check_dep, credentials_dep],
    c_args : c_args,
    cpp_args : cpp_args,
  )

  env = environment()
  env.set('GST_PLUGIN_PATH_1_0', meson.project_build_root())
  env.set('GST_DEBUG', 'check:TRACE,s3sink:TRACE')
  test(test_name, exe, timeout: 3 * 60, env: env)
endforeach
