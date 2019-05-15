gst_s3_elements_sources = [
  'gsts3elements.c',
  'gsts3sink.c',
  'gsts3uploader.c'
]

multipart_uploader = static_library('multipartuploader',
  ['gsts3multipartuploader.cpp'],
  dependencies : [aws_cpp_sdk_s3_dep, aws_cpp_sdk_sts_dep, glib_dep],
  install : false)

multipart_uploader_dep = declare_dependency(link_with : multipart_uploader,
  include_directories : [include_directories('.')]
)

gst_s3_elements = library('gsts3elements',
  gst_s3_elements_sources,
  dependencies : [gst_dep, gst_base_dep, multipart_uploader_dep],
  include_directories : [configinc],
  install : true,
  install_dir : plugins_install_dir,
  override_options : ['cpp_std=c++98', 'c_std=c99'])

s3elements_dep = declare_dependency(link_with : gst_s3_elements,
  include_directories : [include_directories('.')],
  dependencies : [gst_dep, gst_base_dep]
)
