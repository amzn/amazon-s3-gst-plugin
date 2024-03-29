project('amazon-s3-gst-plugin', 'c', 'cpp',
  version : '0.2.0',
  default_options : [ 'warning_level=2',
                      'buildtype=debugoptimized' ])

gst_req = '>= 1.0.0'
aws_cpp_sdk_req = '>= 1.10.30'

gst_s3_version = meson.project_version()

apiversion = '1.0'

is_macos = (host_machine.system() == 'darwin')

glib_dep = dependency('glib-2.0')
gst_dep = dependency('gstreamer-1.0', version : gst_req,
  fallback : ['gstreamer', 'gst_dep'])
gst_base_dep = dependency('gstreamer-base-1.0', version : gst_req,
  fallback : ['gstreamer', 'gst_base_dep'])
gst_check_dep = dependency('gstreamer-check-1.0', version : gst_req,
  fallback : ['gstreamer', 'gst_check_dep'])
cpp = meson.get_compiler('cpp')
aws_c_common_dep = cpp.find_library('aws-c-common', required: true)
aws_crt_cpp_dep=cpp.find_library('aws-crt-cpp', required:true)
# use static linker args on macos as the dylibs don't include the crt symbols
aws_cpp_sdk_s3_dep = dependency('aws-cpp-sdk-s3', version : aws_cpp_sdk_req, static : is_macos)
aws_cpp_sdk_sts_dep = dependency('aws-cpp-sdk-sts', version : aws_cpp_sdk_req, static : is_macos)

configinc = include_directories('.')

plugins_install_dir = join_paths(get_option('libdir'), 'gstreamer-1.0')

core_conf = configuration_data()
core_conf.set_quoted('VERSION', gst_s3_version)
core_conf.set_quoted('PACKAGE', 'amazon-s3-gst-plugin')

configure_file(output : 'config.h', configuration : core_conf)

subdir('src')
subdir('tests')
subdir('pkgconfig')
