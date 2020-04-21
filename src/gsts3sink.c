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

/**
 * SECTION:element-s3sink
 * @title: s3sink
 *
 * Write incoming data to a file in the Amazon S3 bucket.
 *
 * ## Example launch line
 * |[
 * gst-launch-1.0 v4l2src num-buffers=1 ! jpegenc ! s3sink bucket=test-bucket key=myfile.jpg
 * ]| Capture one frame from a v4l2 camera and save as jpeg image.
 *
 */
#ifdef HAVE_CONFIG_H
#  include "config.h"
#endif

#include <string.h>

#include <gst/gst.h>

#include "gsts3sink.h"
#include "gsts3multipartuploader.h"

static GstStaticPadTemplate sinktemplate = GST_STATIC_PAD_TEMPLATE ("sink",
    GST_PAD_SINK,
    GST_PAD_ALWAYS,
    GST_STATIC_CAPS_ANY);

GST_DEBUG_CATEGORY (gst_s3_sink_debug);
#define GST_CAT_DEFAULT gst_s3_sink_debug

#define MIN_BUFFER_SIZE 5 * 1024 * 1024
#define DEFAULT_BUFFER_SIZE GST_S3_UPLOADER_CONFIG_DEFAULT_BUFFER_SIZE
#define DEFAULT_BUFFER_COUNT GST_S3_UPLOADER_CONFIG_DEFAULT_BUFFER_COUNT

enum
{
  PROP_0,
  PROP_BUCKET,
  PROP_KEY,
  PROP_CONTENT_TYPE,
  PROP_CA_FILE,
  PROP_REGION,
  PROP_BUFFER_SIZE,
  PROP_INIT_AWS_SDK,
  PROP_CREDENTIALS,
  PROP_LAST
};

static void gst_s3_sink_dispose (GObject * object);

static void gst_s3_sink_set_property (GObject * object, guint prop_id,
    const GValue * value, GParamSpec * pspec);
static void gst_s3_sink_get_property (GObject * object, guint prop_id,
    GValue * value, GParamSpec * pspec);

static gboolean gst_s3_sink_start (GstBaseSink * sink);
static gboolean gst_s3_sink_stop (GstBaseSink * sink);
static gboolean gst_s3_sink_event (GstBaseSink * sink, GstEvent * event);
static GstFlowReturn gst_s3_sink_render (GstBaseSink * sink,
    GstBuffer * buffer);
static gboolean gst_s3_sink_query (GstBaseSink * bsink, GstQuery * query);

static gboolean gst_s3_sink_fill_buffer (GstS3Sink * sink, GstBuffer * buffer);
static gboolean gst_s3_sink_flush_buffer (GstS3Sink * sink);

#define _do_init \
  GST_DEBUG_CATEGORY_INIT (gst_s3_sink_debug, "s3sink", 0, "s3sink element");
#define gst_s3_sink_parent_class parent_class
G_DEFINE_TYPE_WITH_CODE (GstS3Sink, gst_s3_sink, GST_TYPE_BASE_SINK, _do_init)

static void
gst_s3_sink_class_init (GstS3SinkClass * klass)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (klass);
  GstElementClass *gstelement_class = GST_ELEMENT_CLASS (klass);
  GstBaseSinkClass *gstbasesink_class = GST_BASE_SINK_CLASS (klass);

  gobject_class->dispose = gst_s3_sink_dispose;
  gobject_class->set_property = gst_s3_sink_set_property;
  gobject_class->get_property = gst_s3_sink_get_property;

  g_object_class_install_property (gobject_class, PROP_BUCKET,
      g_param_spec_string ("bucket", "S3 bucket",
          "The bucket of the file to write", NULL,
          G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property (gobject_class, PROP_KEY,
      g_param_spec_string ("location", "S3 key",
          "The key of the file to write", NULL,
          G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property (gobject_class, PROP_CONTENT_TYPE,
      g_param_spec_string ("content-type", "Content type",
          "The content type of a stream", NULL,
          G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property (gobject_class, PROP_CA_FILE,
      g_param_spec_string ("ca-file", "CA file",
          "A path to a CA file", NULL,
          G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

g_object_class_install_property (gobject_class, PROP_REGION,
      g_param_spec_string ("region", "AWS Region",
          "An AWS region (e.g. eu-west-2). Leave empty for region-autodetection "
          "(Please note region-autodetection requires an extra network call)", NULL,
          G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property (gobject_class, PROP_BUFFER_SIZE,
      g_param_spec_uint ("buffer-size", "Buffering size",
          "Size of buffer in number of bytes", MIN_BUFFER_SIZE,
          G_MAXUINT, DEFAULT_BUFFER_SIZE,
          G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property (gobject_class, PROP_INIT_AWS_SDK,
      g_param_spec_boolean ("init-aws-sdk", "Init AWS SDK",
          "Whether to initialize AWS SDK",
          GST_S3_UPLOADER_CONFIG_DEFAULT_INIT_AWS_SDK,
          G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property (gobject_class, PROP_CREDENTIALS,
      g_param_spec_boxed ("aws-credentials", "AWS credentials",
          "The AWS credentials to use", GST_TYPE_AWS_CREDENTIALS,
          G_PARAM_WRITABLE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  gst_element_class_set_static_metadata (gstelement_class,
      "S3 Sink",
      "Sink/S3", "Write stream to an Amazon S3 bucket",
      "Marcin Kolny <marcin.kolny at gmail.com>");
  gst_element_class_add_static_pad_template (gstelement_class, &sinktemplate);

  gstbasesink_class->start = GST_DEBUG_FUNCPTR (gst_s3_sink_start);
  gstbasesink_class->stop = GST_DEBUG_FUNCPTR (gst_s3_sink_stop);
  gstbasesink_class->query = GST_DEBUG_FUNCPTR (gst_s3_sink_query);
  gstbasesink_class->render = GST_DEBUG_FUNCPTR (gst_s3_sink_render);
  gstbasesink_class->event = GST_DEBUG_FUNCPTR (gst_s3_sink_event);
}

static void
gst_s3_destroy_uploader (GstS3Sink * sink)
{
  if (sink->uploader) {
    gst_s3_uploader_destroy (sink->uploader);
    sink->uploader = NULL;
  }
}

static void
gst_s3_sink_init (GstS3Sink * s3sink)
{
  s3sink->config = GST_S3_UPLOADER_CONFIG_INIT;
  s3sink->config.credentials = gst_aws_credentials_new_default ();
  s3sink->uploader = NULL;
  s3sink->is_started = FALSE;

  gst_base_sink_set_sync (GST_BASE_SINK (s3sink), FALSE);
}

static void
gst_s3_sink_release_config (GstS3UploaderConfig * config)
{
  g_free (config->region);
  g_free (config->bucket);
  g_free (config->key);
  g_free (config->content_type);
  g_free (config->ca_file);
  gst_aws_credentials_free (config->credentials);

  *config = GST_S3_UPLOADER_CONFIG_INIT;
}

static void
gst_s3_sink_dispose (GObject * object)
{
  GstS3Sink *sink = GST_S3_SINK (object);

  gst_s3_sink_release_config (&sink->config);

  gst_s3_destroy_uploader (sink);

  G_OBJECT_CLASS (parent_class)->dispose (object);
}

static void
gst_s3_sink_set_string_property (GstS3Sink * sink, const gchar * value,
    gchar ** property, const gchar * property_name)
{
  if (sink->is_started) {
    GST_WARNING ("Changing the `%s' property on s3sink "
        "when streaming has started is not supported.", property_name);
    return;
  }

  g_free (*property);

  if (value != NULL) {
    *property = g_strdup (value);
    GST_INFO_OBJECT (sink, "%s : %s", property_name, *property);
  } else {
    *property = NULL;
  }
}

static void
gst_s3_sink_set_property (GObject * object, guint prop_id,
    const GValue * value, GParamSpec * pspec)
{
  GstS3Sink *sink = GST_S3_SINK (object);

  switch (prop_id) {
    case PROP_BUCKET:
      gst_s3_sink_set_string_property (sink, g_value_get_string (value),
          &sink->config.bucket, "bucket");
      break;
    case PROP_KEY:
      gst_s3_sink_set_string_property (sink, g_value_get_string (value),
          &sink->config.key, "location");
      break;
    case PROP_CONTENT_TYPE:
      gst_s3_sink_set_string_property (sink, g_value_get_string (value),
          &sink->config.content_type, "content-type");
      break;
    case PROP_CA_FILE:
      gst_s3_sink_set_string_property (sink, g_value_get_string (value),
          &sink->config.ca_file, "ca-file");
      break;
    case PROP_REGION:
      gst_s3_sink_set_string_property (sink, g_value_get_string (value),
          &sink->config.region, "region");
      break;
    case PROP_BUFFER_SIZE:
      if (sink->is_started) {
        // TODO: this could be supported in the future
        GST_WARNING
            ("Changing buffer-size property after starting the element is not supported yet.");
      } else {
        sink->config.buffer_size = g_value_get_uint (value);
      }
      break;
    case PROP_INIT_AWS_SDK:
      sink->config.init_aws_sdk = g_value_get_boolean (value);
      break;
    case PROP_CREDENTIALS:
      if (sink->config.credentials)
        gst_aws_credentials_free (sink->config.credentials);
      sink->config.credentials = gst_aws_credentials_copy (g_value_get_boxed (value));
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
      break;
  }
}

static void
gst_s3_sink_get_property (GObject * object, guint prop_id, GValue * value,
    GParamSpec * pspec)
{
  GstS3Sink *sink = GST_S3_SINK (object);

  switch (prop_id) {
    case PROP_BUCKET:
      g_value_set_string (value, sink->config.bucket);
      break;
    case PROP_KEY:
      g_value_set_string (value, sink->config.key);
      break;
    case PROP_CONTENT_TYPE:
      g_value_set_string (value, sink->config.content_type);
      break;
    case PROP_CA_FILE:
      g_value_set_string (value, sink->config.ca_file);
      break;
    case PROP_REGION:
      g_value_set_string (value, sink->config.region);
      break;
    case PROP_BUFFER_SIZE:
      g_value_set_uint (value, sink->config.buffer_size);
      break;
    case PROP_INIT_AWS_SDK:
      g_value_set_boolean (value, sink->config.init_aws_sdk);
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
      break;
  }
}

static gboolean
gst_s3_sink_is_null_or_empty (const gchar * str)
{
  return str == NULL || str[0] == '\0';
}

static gboolean
gst_s3_sink_start (GstBaseSink * basesink)
{
  GstS3Sink *sink = GST_S3_SINK (basesink);

  if (gst_s3_sink_is_null_or_empty (sink->config.bucket)
      || gst_s3_sink_is_null_or_empty (sink->config.key))
    goto no_destination;

  if (sink->uploader == NULL) {
    sink->uploader = gst_s3_multipart_uploader_new (&sink->config);
  }

  if (!sink->uploader)
    goto init_failed;

  g_free (sink->buffer);
  sink->buffer = NULL;

  sink->buffer = g_malloc (sink->config.buffer_size);
  sink->current_buffer_size = 0;
  sink->total_bytes_written = 0;

  GST_DEBUG_OBJECT (sink, "started S3 upload %s %s",
      sink->config.bucket, sink->config.key);

  sink->is_started = TRUE;

  return TRUE;

  /* ERRORS */
no_destination:
  {
    GST_ELEMENT_ERROR (sink, RESOURCE, NOT_FOUND,
        ("No bucket or key specified for writing."), (NULL));
    return FALSE;
  }

init_failed:
  {
    gst_s3_destroy_uploader (sink);
    GST_ELEMENT_ERROR (sink, RESOURCE, OPEN_WRITE,
        ("Unable to initialize S3 uploader."), (NULL));
    return FALSE;
  }
}

static gboolean
gst_s3_sink_stop (GstBaseSink * basesink)
{
  GstS3Sink *sink = GST_S3_SINK (basesink);
  gboolean ret = TRUE;

  if (sink->buffer) {
    gst_s3_sink_flush_buffer (sink);
    ret = gst_s3_uploader_complete (sink->uploader);

    g_free (sink->buffer);
    sink->buffer = NULL;
    sink->current_buffer_size = 0;
    sink->total_bytes_written = 0;
  }

  gst_s3_destroy_uploader (sink);

  sink->is_started = FALSE;

  return ret;
}

static gboolean
gst_s3_sink_query (GstBaseSink * base_sink, GstQuery * query)
{
  gboolean ret = FALSE;
  GstS3Sink *sink;

  sink = GST_S3_SINK (base_sink);

  switch (GST_QUERY_TYPE (query)) {
    case GST_QUERY_FORMATS:
      gst_query_set_formats (query, 2, GST_FORMAT_DEFAULT, GST_FORMAT_BYTES);
      ret = TRUE;
      break;

    case GST_QUERY_POSITION:
    {
      GstFormat format;

      gst_query_parse_position (query, &format, NULL);

      switch (format) {
        case GST_FORMAT_DEFAULT:
        case GST_FORMAT_BYTES:
          gst_query_set_position (query, GST_FORMAT_BYTES,
              sink->total_bytes_written);
          ret = TRUE;
          break;
        default:
          break;
      }
      break;
    }

    case GST_QUERY_SEEKING:{
      GstFormat fmt;

      gst_query_parse_seeking (query, &fmt, NULL, NULL, NULL);
      gst_query_set_seeking (query, fmt, FALSE, 0, -1);
      ret = TRUE;
      break;
    }
    default:
      ret = GST_BASE_SINK_CLASS (parent_class)->query (base_sink, query);
      break;
  }
  return ret;
}

static gboolean
gst_s3_sink_event (GstBaseSink * base_sink, GstEvent * event)
{
  GstEventType type;
  GstS3Sink *sink;

  sink = GST_S3_SINK (base_sink);
  type = GST_EVENT_TYPE (event);

  switch (type) {
    case GST_EVENT_EOS:
      gst_s3_sink_flush_buffer (sink);
      break;
    default:
      break;
  }

  return GST_BASE_SINK_CLASS (parent_class)->event (base_sink, event);
}

static GstFlowReturn
gst_s3_sink_render (GstBaseSink * base_sink, GstBuffer * buffer)
{
  GstS3Sink *sink;
  GstFlowReturn flow;
  guint8 n_mem;

  sink = GST_S3_SINK (base_sink);

  n_mem = gst_buffer_n_memory (buffer);

  if (n_mem > 0) {
    if (gst_s3_sink_fill_buffer (sink, buffer)) {
      flow = GST_FLOW_OK;
    } else {
      GST_WARNING ("Failed to flush the internal buffer");
      flow = GST_FLOW_ERROR;
    }
  } else {
    flow = GST_FLOW_OK;
  }

  return flow;
}

static gboolean
gst_s3_sink_flush_buffer (GstS3Sink * sink)
{
  gboolean ret = TRUE;

  if (sink->current_buffer_size) {
    ret = gst_s3_uploader_upload_part (sink->uploader, sink->buffer,
        sink->current_buffer_size);
    sink->current_buffer_size = 0;
  }

  return ret;
}

static gboolean
gst_s3_sink_fill_buffer (GstS3Sink * sink, GstBuffer * buffer)
{
  GstMapInfo map_info = GST_MAP_INFO_INIT;
  gsize ptr = 0;
  gsize bytes_to_copy;

  if (!gst_buffer_map (buffer, &map_info, GST_MAP_READ))
    goto map_failed;

  do {
    bytes_to_copy =
        MIN (sink->config.buffer_size - sink->current_buffer_size,
        map_info.size - ptr);
    memcpy (sink->buffer + sink->current_buffer_size, map_info.data + ptr,
        bytes_to_copy);
    sink->current_buffer_size += bytes_to_copy;
    if (sink->current_buffer_size == sink->config.buffer_size) {
      if (!gst_s3_sink_flush_buffer (sink)) {
        return FALSE;
      }
    }
    ptr += bytes_to_copy;
    sink->total_bytes_written += bytes_to_copy;
  } while (ptr < map_info.size);

  gst_buffer_unmap (buffer, &map_info);
  return TRUE;

map_failed:
  {
    GST_ELEMENT_ERROR (sink, RESOURCE, NOT_FOUND,
        ("Failed to map the buffer."), (NULL));
    return FALSE;
  }
}
