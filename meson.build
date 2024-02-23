project('amazon-s3-gst-plugin', 'c', 'cpp',
  version : '0.2.1-lldc.6',
  default_options : [ 'warning_level=2',
                      'buildtype=debugoptimized' ])

gst_req = '>= 1.20.2'
aws_cpp_sdk_req = '>= 1.10.30'

gst_s3_version = meson.project_version()
host_system = host_machine.system()

if host_system == 'windows'
  c_args = ['/std:c17']
  cpp_args = ['/std:c++20']
else
  c_args = ['-std=c17']
  cpp_args = ['-std=c++17', '-fexceptions']
endif

apiversion = '1.0'

glib_dep = dependency('glib-2.0')
gst_dep = dependency('gstreamer-1.0', version : gst_req,
  fallback : ['gstreamer', 'gst_dep'])
gst_base_dep = dependency('gstreamer-base-1.0', version : gst_req,
  fallback : ['gstreamer', 'gst_base_dep'])
gst_check_dep = dependency('gstreamer-check-1.0', version : gst_req,
  fallback : ['gstreamer', 'gst_check_dep'])
aws_cpp_sdk_s3_dep = dependency('aws-cpp-sdk-s3', version : aws_cpp_sdk_req)
aws_cpp_sdk_sts_dep = dependency('aws-cpp-sdk-sts', version : aws_cpp_sdk_req)

configinc = include_directories('.')

plugins_install_dir = join_paths(get_option('libdir'), 'gstreamer-1.0')

core_conf = configuration_data()
core_conf.set_quoted('VERSION', gst_base_dep.version())
core_conf.set_quoted('PACKAGE', 'amazon-s3-gst-plugin')

configure_file(output : 'config.h', configuration : core_conf)

subdir('src')
subdir('tests')
subdir('pkgconfig')
