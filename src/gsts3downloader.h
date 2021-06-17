/* amazon-s3-gst-plugin
 * Copyright (C) 2021 Laerdal Labs, DC
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
#ifndef __GST_S3_DOWNLOADER_H__
#define __GST_S3_DOWNLOADER_H__

#include <glib.h>

#include "gsts3uploaderconfig.h"

G_BEGIN_DECLS

typedef struct _GstS3Downloader GstS3Downloader;

GstS3Downloader *gst_s3_downloader_new (const GstS3UploaderConfig * config);

void gst_s3_downloader_free (GstS3Downloader * downloader);

gsize gst_s3_downloader_download_part (GstS3Downloader *
    downloader, gchar * buffer, gsize first, gsize last);

GType gst_s3_downloader_get_type (void);

#define GST_TYPE_AWS_S3_DOWNLOADER \
  (gst_aws_downloader_get_type())

G_END_DECLS

#endif /* __GST_S3_DOWNLOADER_H__ */
