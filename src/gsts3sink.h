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
#ifndef __GST_S3_SINK_H__
#define __GST_S3_SINK_H__

#include <gst/gst.h>
#include <gst/base/gstbasesink.h>

#include "gsts3uploader.h"
#include "gstawscredentials.h"

G_BEGIN_DECLS

#define GST_TYPE_S3_SINK \
  (gst_s3_sink_get_type())
#define GST_S3_SINK(obj) \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),GST_TYPE_S3_SINK,GstS3Sink))
#define GST_S3_SINK_CLASS(klass) \
  (G_TYPE_CHECK_CLASS_CAST((klass),GST_TYPE_S3_SINK,GstS3SinkClass))
#define GST_IS_S3_SINK(obj) \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),GST_TYPE_S3_SINK))
#define GST_IS_S3_SINK_CLASS(klass) \
  (G_TYPE_CHECK_CLASS_TYPE((klass),GST_TYPE_S3_SINK))
#define GST_S3_SINK_CAST(obj) ((GstS3Sink *)(obj))
typedef struct _GstS3Sink GstS3Sink;
typedef struct _GstS3SinkClass GstS3SinkClass;

/**
 * GstS3Sink:
 *
 * Opaque #GstS3Sink structure.
 */
struct _GstS3Sink {
  GstBaseSink parent;

  /*< private > */
  GstS3UploaderConfig config;

  GstS3Uploader *uploader;

  gchar *buffer;
  gsize current_buffer_size;
  gsize total_bytes_written;

  gboolean is_started;
};

struct _GstS3SinkClass {
  GstBaseSinkClass parent_class;
};

GType gst_s3_sink_get_type (void);

G_END_DECLS

#endif /* __GST_S3_SINK_H__ */
