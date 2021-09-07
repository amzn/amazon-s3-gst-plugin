/* amazon-s3-gst-plugin
 * Copyright (C) 2019 Amazon <mkolny@amazon.com>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 51 Franklin St, Fifth Floor,
 * Boston, MA 02110-1301, USA.
 */
#ifndef __GST_S3_UPLOADER_CONFIG_H__
#define __GST_S3_UPLOADER_CONFIG_H__

#include <glib.h>

#include "gstawscredentials.h"

G_BEGIN_DECLS

#define GST_S3_UPLOADER_CONFIG_DEFAULT_BUFFER_SIZE 5 * 1024 * 1024
#define GST_S3_UPLOADER_CONFIG_DEFAULT_BUFFER_COUNT 4
#define GST_S3_UPLOADER_CONFIG_DEFAULT_INIT_AWS_SDK TRUE
#define GST_S3_UPLOADER_CONFIG_DEFAULT_PROP_AWS_SDK_USE_HTTP FALSE
#define GST_S3_UPLOADER_CONFIG_DEFAULT_PROP_AWS_SDK_VERIFY_SSL TRUE
#define GST_S3_UPLOADER_CONFIG_DEFAULT_PROP_AWS_SDK_S3_SIGN_PAYLOAD TRUE

typedef struct {
  gchar * region;
  gchar * bucket;
  gchar * key;
  gchar * location;
  gchar * acl;
  gchar * content_type;
  gchar * ca_file;
  GstAWSCredentials * credentials;
  gsize buffer_size;
  gsize buffer_count;
  gboolean init_aws_sdk;
  gchar * aws_sdk_endpoint;
  gboolean aws_sdk_use_http;
  gboolean aws_sdk_verify_ssl;
  gboolean aws_sdk_s3_sign_payload;
} GstS3UploaderConfig;

#define GST_S3_UPLOADER_CONFIG_INIT (GstS3UploaderConfig) { \
  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \
  GST_S3_UPLOADER_CONFIG_DEFAULT_BUFFER_SIZE, \
  GST_S3_UPLOADER_CONFIG_DEFAULT_BUFFER_COUNT, \
  GST_S3_UPLOADER_CONFIG_DEFAULT_INIT_AWS_SDK, \
  NULL, \
  GST_S3_UPLOADER_CONFIG_DEFAULT_PROP_AWS_SDK_USE_HTTP, \
  GST_S3_UPLOADER_CONFIG_DEFAULT_PROP_AWS_SDK_VERIFY_SSL, \
  GST_S3_UPLOADER_CONFIG_DEFAULT_PROP_AWS_SDK_S3_SIGN_PAYLOAD \
}

G_END_DECLS

#endif /* __GST_S3_UPLOADER_CONFIG_H__ */
