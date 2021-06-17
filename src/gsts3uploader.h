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
#ifndef __GST_S3_UPLOADER_H__
#define __GST_S3_UPLOADER_H__

#include <glib.h>

#include "gsts3uploaderconfig.h"

G_BEGIN_DECLS

typedef struct _GstS3Uploader GstS3Uploader;

typedef struct {
  void (*destroy) (GstS3Uploader *);
  gboolean (*upload_part) (GstS3Uploader *, const gchar *, gsize);
  gboolean (*upload_part_copy) (GstS3Uploader *, const gchar *, const gchar *, gsize, gsize);
  gboolean (*complete) (GstS3Uploader *);
} GstS3UploaderClass;

struct _GstS3Uploader {
  GstS3UploaderClass *klass;
};

GstS3Uploader *gst_s3_uploader_new_default (const GstS3UploaderConfig * config);

void gst_s3_uploader_destroy (GstS3Uploader * uploader);

gboolean gst_s3_uploader_upload_part (GstS3Uploader *
    uploader, const gchar * buffer, gsize size);

gboolean gst_s3_uploader_upload_part_copy (GstS3Uploader *
    uploader, const gchar * bucket, const gchar * key, gsize first, gsize last);

gboolean gst_s3_uploader_complete (GstS3Uploader * uploader);

G_END_DECLS

#endif /* __GST_S3_UPLOADER_H__ */
