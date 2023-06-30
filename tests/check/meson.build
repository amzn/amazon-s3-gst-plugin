element_tests = ['s3sink.c']

# create a dependency that omits the compiler args because clang refuses
# to compile c files with cpp args
c_safe_s3elements_dep = s3elements_dep.partial_dependency(
  sources: true,
  includes: true,
  links: true
)

foreach test_file : element_tests
  test_name = test_file.split('.').get(0).underscorify()

  exe = executable(test_name, test_file,
    include_directories : [configinc],
    dependencies : [c_safe_s3elements_dep, gst_check_dep]
  )

  env = environment()
  env.set('GST_PLUGIN_PATH_1_0', meson.build_root())
  test(test_name, exe, timeout: 3 * 60, env: env)
endforeach

