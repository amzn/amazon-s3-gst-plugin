element_tests = ['s3sink.c']

element_tests_includes = include_directories('.')

foreach test_file : element_tests
  test_name = test_file.split('.').get(0).underscorify()

  exe = executable(test_name, test_file,
    include_directories : [configinc, element_tests_includes],
    dependencies : [s3elements_dep, gst_check_dep]
  )

  env = environment()
  env.set('GST_PLUGIN_PATH_1_0', meson.build_root())
  test(test_name, exe, timeout: 3 * 60, env: env)
endforeach

