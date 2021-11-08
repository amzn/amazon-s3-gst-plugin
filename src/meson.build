gst_s3_elements_sources = [
  'gsts3elements.c',
  'gsts3sink.c',
  'gsts3uploader.c'
]

gst_s3_public_headers = [
  'gstawscredentials.h',
  'gstawscredentials.hpp'
]

# Export symbols when building, import when not. This is optional on Linux
# / macOS, but necessary on Windows otherwise .lib files will not be generated.
symbol_export_define = ['-DGST_EXPORTS']

credentials = library('gstawscredentials-@0@'.format(apiversion),
  ['gstawscredentials.cpp'],
  dependencies : [aws_cpp_sdk_sts_dep, gst_dep],
  cpp_args: symbol_export_define,
  install : true
)

credentials_dep = declare_dependency(link_with : credentials,
  include_directories : [include_directories('.')]
)

multipart_uploader = static_library('multipartuploader',
  ['gsts3multipartuploader.cpp'],
  dependencies : [aws_cpp_sdk_s3_dep, gst_dep],
  install : false
)

multipart_uploader_dep = declare_dependency(link_with : multipart_uploader,
  include_directories : [include_directories('.')])

gst_s3_elements = library('gsts3elements',
  gst_s3_elements_sources,
  cpp_args: symbol_export_define,
  c_args: symbol_export_define,
  dependencies : [gst_dep, gst_base_dep, multipart_uploader_dep, credentials_dep],
  include_directories : [configinc],
  install : true,
  install_dir : plugins_install_dir,
  override_options : ['cpp_std=c++98', 'c_std=c99'])

s3elements_dep = declare_dependency(link_with : gst_s3_elements,
  include_directories : [include_directories('.')],
  dependencies : [gst_dep, gst_base_dep, aws_cpp_sdk_s3_dep, aws_cpp_sdk_sts_dep]
)

install_headers(gst_s3_public_headers, subdir : 'gstreamer-1.0/gst/aws')
