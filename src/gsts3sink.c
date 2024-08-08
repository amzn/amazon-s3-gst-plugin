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
#include <gst/gsturi.h>

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

#define REQUIRED_BUT_UNUSED(x) (void)(x)

// boilerplate
GstS3SinkClass* GST_S3_SINK_GET_CLASS(gpointer *ptr)
{
  return G_TYPE_INSTANCE_GET_CLASS(ptr, GST_TYPE_S3_SINK, GstS3SinkClass);
}

enum
{
  PROP_0,
  PROP_BUCKET,
  PROP_KEY,
  PROP_LOCATION,
  PROP_ACL,
  PROP_CONTENT_TYPE,
  PROP_CA_FILE,
  PROP_REGION,
  PROP_BUFFER_SIZE,
  PROP_INIT_AWS_SDK,
  PROP_CREDENTIALS,
  PROP_AWS_SDK_ENDPOINT,
  PROP_AWS_SDK_USE_HTTP,
  PROP_AWS_SDK_VERIFY_SSL,
  PROP_AWS_SDK_S3_SIGN_PAYLOAD,
  PROP_AWS_SDK_REQUEST_TIMEOUT,
  PROP_AWS_SDK_RETRY_MAX,
  PROP_AWS_SDK_RETRY_SCALE,
  PROP_NUM_CACHE_PARTS,
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

static gboolean gst_s3_sink_do_flush (GstS3Sink * sink);
static gboolean gst_s3_sink_do_seek (GstS3Sink * sink, guint64 new_offset);
static gboolean gst_s3_sink_fill_buffer (GstS3Sink * sink, GstBuffer * buffer);
static gboolean gst_s3_sink_flush_buffer (GstS3Sink * sink);

/**
 * GstURIHandler Interface implementation
 */
static GstURIType
gst_s3_sink_urihandler_get_type (GType type)
{
  REQUIRED_BUT_UNUSED(type);
  return GST_URI_SINK;
}

static const gchar * const*
gst_s3_sink_urihandler_get_protocols (GType type)
{
  REQUIRED_BUT_UNUSED(type);
  static const gchar *protocols[] = { "s3", NULL};
  return protocols;
}

static gchar *
gst_s3_sink_urihandler_get_uri (GstURIHandler * handler)
{
  GValue value = {0};
  g_object_get_property( G_OBJECT(handler), "location", &value);
  return g_strdup_value_contents(&value);
}

static gboolean
gst_s3_sink_urihandler_set_uri (GstURIHandler * handler, const gchar * uri, GError **error)
{
  REQUIRED_BUT_UNUSED(error);
  g_object_set( G_OBJECT(handler), "location", uri, NULL);
  return TRUE;
}

static void
gst_s3_sink_urihandler_init (gpointer g_iface, gpointer iface_data)
{
  REQUIRED_BUT_UNUSED(iface_data);
  GstURIHandlerInterface *iface = (GstURIHandlerInterface *) g_iface;
  iface->get_type      = gst_s3_sink_urihandler_get_type;
  iface->get_protocols = gst_s3_sink_urihandler_get_protocols;
  iface->get_uri       = gst_s3_sink_urihandler_get_uri;
  iface->set_uri       = gst_s3_sink_urihandler_set_uri;
}

#define gst_s3_sink_parent_class parent_class
G_DEFINE_TYPE_WITH_CODE (GstS3Sink, gst_s3_sink, GST_TYPE_BASE_SINK,
  G_IMPLEMENT_INTERFACE (GST_TYPE_URI_HANDLER, gst_s3_sink_urihandler_init));

static void
gst_s3_sink_class_init (GstS3SinkClass * klass)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (klass);
  GstElementClass *gstelement_class = GST_ELEMENT_CLASS (klass);
  GstBaseSinkClass *gstbasesink_class = GST_BASE_SINK_CLASS (klass);

  GST_DEBUG_CATEGORY_INIT (gst_s3_sink_debug, "s3sink", 0, "s3sink element");

  gobject_class->dispose = gst_s3_sink_dispose;
  gobject_class->set_property = gst_s3_sink_set_property;
  gobject_class->get_property = gst_s3_sink_get_property;

  klass->downloader_new = gst_s3_downloader_new;
  klass->uploader_new = gst_s3_multipart_uploader_new;

  g_object_class_install_property (gobject_class, PROP_BUCKET,
    g_param_spec_string ("bucket", "S3 bucket",
      "The bucket of the file to write (ignored when 'location' is set)", NULL,
      G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property (gobject_class, PROP_KEY,
    g_param_spec_string ("key", "S3 key",
      "The key of the file to write (ignored when 'location' is set)", NULL,
      G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property (gobject_class, PROP_LOCATION,
    g_param_spec_string ("location", "S3 URI",
      "The URI of the file to write", NULL,
      G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property (gobject_class, PROP_ACL,
    g_param_spec_string ("acl", "S3 object acl",
      "The canned acl for s3 object to upload", NULL,
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

  g_object_class_install_property (gobject_class, PROP_AWS_SDK_ENDPOINT,
    g_param_spec_string ("aws-sdk-endpoint", "AWS SDK Endpoint",
      "AWS SDK endpoint override (ip:port)", NULL,
      G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property (gobject_class, PROP_AWS_SDK_USE_HTTP,
    g_param_spec_boolean ("aws-sdk-use-http", "AWS SDK Use HTTP",
      "Whether to enable http for the AWS SDK (default https)",
      GST_S3_UPLOADER_CONFIG_DEFAULT_PROP_AWS_SDK_USE_HTTP,
      G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property (gobject_class, PROP_AWS_SDK_VERIFY_SSL,
    g_param_spec_boolean ("aws-sdk-verify-ssl", "AWS SDK Verify SSL",
      "Whether to enable/disable tls validation for the AWS SDK",
      GST_S3_UPLOADER_CONFIG_DEFAULT_PROP_AWS_SDK_VERIFY_SSL,
      G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property (gobject_class, PROP_AWS_SDK_S3_SIGN_PAYLOAD,
    g_param_spec_boolean ("aws-sdk-s3-sign-payload", "AWS SDK S3 Sign Payload",
      "Whether to have the AWS SDK S3 client sign payloads using the Auth v4 Signer",
      GST_S3_UPLOADER_CONFIG_DEFAULT_PROP_AWS_SDK_S3_SIGN_PAYLOAD,
      G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property (gobject_class,  PROP_AWS_SDK_REQUEST_TIMEOUT,
    g_param_spec_int ("aws-sdk-request-timeout", "AWS SDK Request Timeout (milliseconds)",
      "The request timeout to be used on the uploader and downloader; -1 is ignored",
      GST_S3_UPLOADER_CONFIG_DEFAULT_PROP_AWS_SDK_REQUEST_TIMEOUT, G_MAXINT32, /* min / max */
      GST_S3_UPLOADER_CONFIG_DEFAULT_PROP_AWS_SDK_REQUEST_TIMEOUT,
      G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property (gobject_class,  PROP_AWS_SDK_RETRY_MAX,
    g_param_spec_int ("aws-sdk-retry-max", "AWS SDK Retry Maximum",
      "Maximum number of retries for calls to the API (exponential back-off)",
      0, G_MAXINT32, /* min / max */
      GST_S3_UPLOADER_CONFIG_DEFAULT_PROP_AWS_SDK_RETRY_MAX,
      G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property (gobject_class,  PROP_AWS_SDK_RETRY_SCALE,
    g_param_spec_int ("aws-sdk-retry-scale", "AWS SDK Retry Scale",
      "Retry scale for exponential back-off strategy",
      1, G_MAXINT32, /* min / max */
      GST_S3_UPLOADER_CONFIG_DEFAULT_PROP_AWS_SDK_RETRY_SCALE,
      G_PARAM_READWRITE | GST_PARAM_MUTABLE_READY | G_PARAM_STATIC_STRINGS));

  g_object_class_install_property (gobject_class,  PROP_NUM_CACHE_PARTS,
    g_param_spec_int ("num-cache-parts", "Number of Parts to cache in uploader",
      "0 is no cache; [1,10000] cache first N parts, [-10000,-1] last N parts",
      -10000, 10000, 0, /* min / max / default*/
      G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS));

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

static void gst_s3_destroy_downloader (GstS3Sink * sink)
{
  if (sink->downloader) {
    gst_s3_downloader_destroy (sink->downloader);
    sink->downloader = NULL;
  }
}

static void
gst_s3_sink_init (GstS3Sink * s3sink)
{
  s3sink->config = GST_S3_UPLOADER_CONFIG_INIT;
  s3sink->uploader = NULL;
  s3sink->downloader = NULL;
  s3sink->is_started = FALSE;
  s3sink->buffer_from_cache = FALSE;
  s3sink->becoming_eos = FALSE;
  s3sink->uploader_needs_complete = FALSE;

  gst_base_sink_set_sync (GST_BASE_SINK (s3sink), FALSE);
}

static void
gst_s3_sink_release_config (GstS3UploaderConfig * config)
{
  g_free (config->region);
  g_free (config->bucket);
  g_free (config->key);
  g_free (config->location);
  g_free (config->acl);
  g_free (config->content_type);
  g_free (config->ca_file);
  g_free (config->aws_sdk_endpoint);
  if (config->credentials)
    gst_aws_credentials_free (config->credentials);

  *config = GST_S3_UPLOADER_CONFIG_INIT;
}

static void
gst_s3_sink_dispose (GObject * object)
{
  GstS3Sink *sink = GST_S3_SINK (object);

  gst_s3_destroy_uploader (sink);
  gst_s3_destroy_downloader (sink);

  gst_s3_sink_release_config (&sink->config);

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
        &sink->config.key, "key");
      break;
    case PROP_LOCATION:
      gst_s3_sink_set_string_property (sink, g_value_get_string (value),
        &sink->config.location, "location");
      break;
    case PROP_ACL:
      gst_s3_sink_set_string_property (sink, g_value_get_string (value),
        &sink->config.acl, "acl");
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
      GST_DEBUG ("Setting AWS credentials");
      sink->config.credentials = gst_aws_credentials_copy (g_value_get_boxed (value));
      break;
    case PROP_AWS_SDK_ENDPOINT:
      gst_s3_sink_set_string_property (sink, g_value_get_string (value),
        &sink->config.aws_sdk_endpoint, "aws-sdk-endpoint");
      break;
    case PROP_AWS_SDK_USE_HTTP:
      sink->config.aws_sdk_use_http = g_value_get_boolean (value);
      break;
    case PROP_AWS_SDK_VERIFY_SSL:
      sink->config.aws_sdk_verify_ssl = g_value_get_boolean (value);
      break;
    case PROP_AWS_SDK_S3_SIGN_PAYLOAD:
      sink->config.aws_sdk_s3_sign_payload = g_value_get_boolean (value);
      break;
    case PROP_AWS_SDK_REQUEST_TIMEOUT:
      sink->config.aws_sdk_request_timeout_ms = g_value_get_int (value);
      break;
    case PROP_AWS_SDK_RETRY_MAX:
      sink->config.aws_sdk_retry_max = g_value_get_int (value);
      break;
    case PROP_AWS_SDK_RETRY_SCALE:
      sink->config.aws_sdk_retry_scale = g_value_get_int (value);
      break;
    case PROP_NUM_CACHE_PARTS:
      if (sink->is_started) {
        GST_WARNING("Changing num-cache-parts property after starting the element is not supported yet.");
      } else {
        sink->config.cache_num_parts = g_value_get_int (value);
      }
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
    case PROP_LOCATION:
      g_value_set_string (value, sink->config.location);
      break;
    case PROP_ACL:
      g_value_set_string (value, sink->config.acl);
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
    case PROP_AWS_SDK_ENDPOINT:
      g_value_set_string (value, sink->config.aws_sdk_endpoint);
      break;
    case PROP_AWS_SDK_USE_HTTP:
      g_value_set_boolean (value, sink->config.aws_sdk_use_http);
      break;
    case PROP_AWS_SDK_VERIFY_SSL:
      g_value_set_boolean (value, sink->config.aws_sdk_verify_ssl);
      break;
    case PROP_AWS_SDK_S3_SIGN_PAYLOAD:
      g_value_set_boolean (value, sink->config.aws_sdk_s3_sign_payload);
      break;
    case PROP_AWS_SDK_REQUEST_TIMEOUT:
      g_value_set_int (value, sink->config.aws_sdk_request_timeout_ms);
      break;
    case PROP_AWS_SDK_RETRY_MAX:
      g_value_set_int (value, sink->config.aws_sdk_retry_max);
      break;
    case PROP_AWS_SDK_RETRY_SCALE:
      g_value_set_int (value, sink->config.aws_sdk_retry_scale);
      break;
    case PROP_NUM_CACHE_PARTS:
      g_value_set_int (value, sink->config.cache_num_parts);
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

  if (gst_s3_sink_is_null_or_empty (sink->config.location) && (
      gst_s3_sink_is_null_or_empty (sink->config.bucket)
      || gst_s3_sink_is_null_or_empty (sink->config.key)))
    goto no_destination;

  if (!sink->config.credentials) {
    GST_DEBUG ("Using default AWS credentials");
    sink->config.credentials = gst_aws_credentials_new_default ();
  }

  if (sink->uploader == NULL) {
    sink->uploader = GST_S3_SINK_GET_CLASS((gpointer*) sink)->uploader_new (&sink->config);
  }

  if (sink->downloader == NULL) {
    sink->downloader = GST_S3_SINK_GET_CLASS((gpointer *) sink)->downloader_new (&sink->config);
  }

  if (!sink->uploader)
    goto init_failed;

  g_free (sink->buffer);
  sink->buffer = NULL;

  sink->buffer = g_malloc (sink->config.buffer_size);
  sink->buffer_size = sink->buffer_pos = 0;
  sink->upload_size = sink->current_pos = 0;

  if ( gst_s3_sink_is_null_or_empty (sink->config.location) )
  {
    GST_DEBUG_OBJECT (sink, "started S3 upload %s %s",
      sink->config.bucket, sink->config.key);
  }
  else
  {
    GST_DEBUG_OBJECT (sink, "started S3 upload %s", sink->config.location);
  }

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
    gst_s3_destroy_downloader (sink);
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

  gst_s3_sink_do_flush (sink);
  if (sink->buffer) {
    g_free (sink->buffer);
    sink->buffer = NULL;
  }

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
            sink->current_pos);
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
      if (fmt == GST_FORMAT_BYTES || fmt == GST_FORMAT_DEFAULT) {
        gst_query_set_seeking (query, GST_FORMAT_BYTES, TRUE, 0, -1);
      } else {
        gst_query_set_seeking (query, fmt, FALSE, 0, -1);
      }
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
    case GST_EVENT_SEGMENT:{
      const GstSegment *segment;

      gst_event_parse_segment (event, &segment);

      if (segment->format == GST_FORMAT_BYTES) {
        if (sink->current_pos != segment->start) {
          if (!gst_s3_sink_do_seek(sink, segment->start))
            goto seek_failed;
        }
      } else {
        GST_WARNING_OBJECT (sink,
          "Ignored SEGMENT event of format %u (%s)", (guint) segment->format,
          gst_format_get_name (segment->format));
      }
      break;
    }
    case GST_EVENT_FLUSH_STOP:
      //TODO: truncate to 0 bytes (is this necessary?)
      break;
    case GST_EVENT_EOS:
      sink->becoming_eos = TRUE;
      gst_s3_sink_do_flush (sink);
      gst_s3_uploader_complete(sink->uploader);
      gst_s3_destroy_uploader(sink);
      break;
    default:
      break;
  }

  return GST_BASE_SINK_CLASS (parent_class)->event (base_sink, event);

  seek_failed:
    {
      GST_ELEMENT_ERROR (sink, RESOURCE, SEEK,
        (("Error while seeking S3 Upload \"%s/%s\"."), sink->config.bucket, sink->config.key),
        GST_ERROR_SYSTEM);
      gst_event_unref (event);
      return FALSE;
    }
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

  if (sink->buffer_size) {
    gboolean was_from_cache = sink->buffer_from_cache;
    gchar *next = NULL;
    gsize next_size = 0;
    GST_DEBUG_OBJECT(sink, "Uploading %ld byte part", sink->buffer_size);
    ret = gst_s3_uploader_upload_part (sink->uploader, sink->buffer,
      sink->buffer_size, &next, &next_size);

    sink->buffer_from_cache = FALSE;
    if (next) {
      if (next_size <= sink->config.buffer_size) {
        GST_DEBUG_OBJECT(sink, "upload_part returned next cached part; copying.");
        memcpy(sink->buffer, next, next_size);
        sink->buffer_from_cache = TRUE;
      }
      else {
        GST_DEBUG_OBJECT(sink, "upload_part resulted in cached buffer larger than allowed configured size; discarding.");
      }
      g_free(next);
    }
    else if (was_from_cache && !sink->becoming_eos) {
      // Any future operation where a download or copy-upload need to take place will require
      // re-syncing the uploader and local state FIRST.
      sink->uploader_needs_complete = TRUE;
    }
    sink->buffer_pos = sink->buffer_size = 0;
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
      MIN (sink->config.buffer_size - sink->buffer_pos,
      map_info.size - ptr);
    memcpy (sink->buffer + sink->buffer_pos, map_info.data + ptr,
      bytes_to_copy);
    sink->buffer_pos += bytes_to_copy;
    sink->buffer_size = MAX(sink->buffer_pos, sink->buffer_size);
    if (sink->buffer_pos == sink->config.buffer_size) {
      if (!gst_s3_sink_flush_buffer (sink)) {
        return FALSE;
      }
    }
    ptr += bytes_to_copy;
    sink->current_pos += bytes_to_copy;
    sink->upload_size = MAX(sink->upload_size, sink->current_pos);
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

static gboolean
gst_s3_sink_do_flush (GstS3Sink * sink)
{
  gsize bytes_remaining;
  gboolean ret = TRUE;

  if (sink->uploader && sink->buffer) {
    GST_DEBUG_OBJECT (sink, "Flushing S3 Upload");

    // Set current position to end of data in current buffer
    const gsize buffer_start = sink->current_pos - sink->buffer_pos;
    if (sink->buffer_from_cache) // potentially bad assumption about to happen
      sink->buffer_size = sink->config.buffer_size;
    sink->buffer_pos = sink->buffer_size;
    sink->current_pos = buffer_start + sink->buffer_pos;

    if (sink->uploader_needs_complete) {
      GST_DEBUG_OBJECT(sink, "uploader needs completion; doing that then copy-upload to buffer start");
      gst_s3_uploader_complete(sink->uploader);
      gst_s3_destroy_uploader(sink);
      sink->uploader = GST_S3_SINK_GET_CLASS((gpointer*) sink)->uploader_new (&sink->config);
      sink->uploader_needs_complete = FALSE;

      // copy-upload from 0->buffer_start - 1
      gst_s3_uploader_upload_part_copy(
        sink->uploader,
        sink->config.bucket,
        sink->config.key,
        0,
        buffer_start - 1
      );
    }

    if (!sink->buffer_from_cache &&
        sink->buffer_size != sink->config.buffer_size &&
        sink->current_pos < sink->upload_size)
    {
      const gsize bytes_to_read = MIN(sink->config.buffer_size - sink->buffer_size, sink->upload_size - sink->current_pos);
      GST_TRACE_OBJECT(sink, "Post-filling %ld bytes, range: %ld-%ld", bytes_to_read, sink->current_pos,
        sink->current_pos + bytes_to_read);

      const gsize bytes_read = gst_s3_downloader_download_part(
        sink->downloader,
        sink->buffer + sink->buffer_pos,
        sink->current_pos,
        sink->current_pos + bytes_to_read);

      sink->buffer_size += bytes_read;
      sink->buffer_pos += bytes_read;
      sink->current_pos += bytes_read;

      if (bytes_to_read != bytes_read) {
        GST_WARNING_OBJECT(sink, "Failed to post-fill %ld bytes, only read %ld", bytes_to_read, bytes_read);
      }
    }

    gst_s3_sink_flush_buffer (sink);

    bytes_remaining = sink->upload_size - sink->current_pos;
    if (bytes_remaining) {
      // The following strictly happens at EOS/stop to push "the rest of the file", which depending on our situation
      // may or may not be necessary.  We use the uploader to figure out the most efficient path.
      char *next = NULL;
      gsize next_size = 0;

      gst_s3_uploader_seek(sink->uploader, sink->upload_size - 1, &next, &next_size);
      if (next)
        g_free(next);
      if (next_size > 0) {
        // do_flush is the way we got here for this specific case, so since there
        // is a known end to the file, "soft seek" to it so that the do_flush does
        // not attempt to copy-upload the same data over top of itself.
        GST_TRACE_OBJECT(sink, "Remaining file is known to uploader; soft-seek to EOF");
      }
      else if (bytes_remaining < sink->config.buffer_size) {
        GST_TRACE_OBJECT(sink, "Re-uploading remaining file from %ld to %ld", sink->current_pos, sink->upload_size);
        const gsize bytes_read = gst_s3_downloader_download_part(
          sink->downloader,
          sink->buffer + sink->buffer_pos,
          sink->current_pos,
          sink->upload_size);

        sink->buffer_size += bytes_read;
        sink->buffer_pos += bytes_read;
        sink->current_pos += bytes_read;

        if (bytes_remaining != bytes_read) {
          GST_WARNING_OBJECT(sink, "Failed to re-upload %ld bytes, only read %ld", bytes_remaining, bytes_read);
        }

        gst_s3_sink_flush_buffer (sink);
      } else {
        GST_TRACE_OBJECT(sink, "Copy-uploading remaining file from %ld to %ld", sink->current_pos, sink->upload_size);
        gst_s3_uploader_upload_part_copy(
          sink->uploader,
          sink->config.bucket,
          sink->config.key,
          sink->current_pos,
          sink->upload_size - 1
        );
      }
    }

    sink->current_pos = sink->upload_size;
  }

  return ret;
}

static gboolean
gst_s3_sink_do_seek (GstS3Sink * sink, guint64 new_offset)
{
  gsize bytes_to_zero;

  GST_DEBUG_OBJECT(sink, "Seeking to new offset %" G_GUINT64_FORMAT " from %"
    G_GUINT64_FORMAT " of %" G_GUINT64_FORMAT " total bytes", new_offset,
    sink->current_pos, sink->upload_size);

  const gsize buffer_start = sink->current_pos - sink->buffer_pos;
  const gsize buffer_end = buffer_start + sink->config.buffer_size;

  if (new_offset >= buffer_start && new_offset < buffer_end) {
    GST_TRACE_OBJECT (sink, "Seeking to offset %" G_GUINT64_FORMAT
      " which is within current buffer", new_offset);

    sink->current_pos = MIN(buffer_start + sink->buffer_size, new_offset);
    sink->buffer_pos = sink->current_pos - buffer_start;
  }
  else if (sink->current_pos != sink->upload_size || new_offset < sink->current_pos) {
    gsize new_pos;
    gchar *next = NULL;
    gsize next_size = 0;

    if (!gst_s3_sink_do_flush(sink))
      goto flush_failed;

    new_pos = MIN(new_offset, sink->upload_size);

    if (gst_s3_uploader_seek(sink->uploader, new_offset, &next, &next_size)) {
      // todo: something in this situation.  Some notes...
      // 1. We do not need to 'complete' or destroy the uploader since we're working
      //    from cache.
      // 2. The _do_flush(sink) will have called _flush_buffer, which calls _upload_part
      //    and gets the next buffer, if found, which is the buffer after the one we just
      //    seeked away from, in whatever direction.
      // 3. 'next' will be a valid buffer with a non-zero size, guaranteed, and the
      //    uploader's part counter is already changed to reflect the move to this offset.
      GST_TRACE_OBJECT (sink, "Seeking to offset %" G_GUINT64_FORMAT
          " using local cache", new_offset);
      if (next_size > sink->config.buffer_size)
        goto cache_failed;

      memcpy(sink->buffer, next, next_size);
      if (*next)
        g_free(next);

      // Assumption here is all preceding buffers, if they exist,
      // are the same size as the config states.
      sink->buffer_from_cache = TRUE;
      sink->current_pos = new_offset;
      sink->buffer_pos = new_offset % sink->config.buffer_size;
    }
    else {
      gst_s3_uploader_complete(sink->uploader);
      gst_s3_destroy_uploader(sink);
      sink->uploader = GST_S3_SINK_GET_CLASS((gpointer*) sink)->uploader_new (&sink->config);
      sink->uploader_needs_complete = FALSE;

      if (new_pos >= sink->config.buffer_size) {
        gboolean res;

        GST_TRACE_OBJECT (sink, "Seeking to offset %" G_GUINT64_FORMAT
          " using multipart upload copy", new_offset);
        res = gst_s3_uploader_upload_part_copy(
          sink->uploader,
          sink->config.bucket,
          sink->config.key,
          0,
          new_pos - 1
        );

        if (!res) {
          goto upload_copy_failed;
        }

        sink->current_pos = new_pos;
      }
      else if (new_pos > 0) {
        GST_TRACE_OBJECT (sink, "Seeking to offset %" G_GUINT64_FORMAT
          " by downloading", new_offset);

        const gsize bytes_read = gst_s3_downloader_download_part(
          sink->downloader,
          sink->buffer,
          0,
          new_pos);

        sink->current_pos = sink->buffer_size = sink->buffer_pos = bytes_read;

        if (bytes_read != new_pos) {
          GST_WARNING_OBJECT(sink, "Failed to preload %ld bytes, only read %ld", new_pos, bytes_read);
        }
      }
    }
  }

  while (sink->current_pos < new_offset)
  {
    bytes_to_zero =
      MIN(sink->config.buffer_size - sink->buffer_size,
      new_offset - sink->current_pos);
    memset(sink->buffer + sink->buffer_pos, 0, bytes_to_zero);
    sink->buffer_size += bytes_to_zero;
    sink->buffer_pos += bytes_to_zero;
    if (sink->buffer_size == sink->config.buffer_size) {
      if (!gst_s3_sink_flush_buffer (sink)) {
        return FALSE;
      }
    }
    sink->current_pos += bytes_to_zero;
    sink->upload_size = MAX(sink->upload_size, sink->current_pos);
  }

  return TRUE;

  /* ERRORS */
cache_failed:
  {
    GST_ERROR_OBJECT (sink, "Cache-based seek failed");
    return FALSE;
  }
flush_failed:
  {
    GST_ERROR_OBJECT (sink, "Flush failed");
    return FALSE;
  }
upload_copy_failed:
  {
    GST_ERROR_OBJECT (sink, "Upload copy failed");
    return FALSE;
  }
}
