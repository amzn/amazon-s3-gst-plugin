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
#include "gsts3uploader.h"
#include "gsts3sink.h"

#include <gst/check/gstcheck.h>

static GstStaticPadTemplate srctemplate = GST_STATIC_PAD_TEMPLATE ("src",
    GST_PAD_SRC,
    GST_PAD_ALWAYS,
    GST_STATIC_CAPS_ANY);

/************* TEST UPLOADER *************/
typedef struct {
    GstS3Uploader base;
    gint fail_upload_retry;
    gboolean fail_complete;

    gint upload_part_count;
} TestUploader;

#define TEST_UPLOADER(uploader) ((TestUploader*) uploader)

static void
test_uploader_destroy (GstS3Uploader * uploader)
{
  g_free(uploader);
}

static gboolean
test_uploader_upload_part (GstS3Uploader * uploader, G_GNUC_UNUSED const gchar * buffer, G_GNUC_UNUSED gsize size)
{
  gboolean ok = TEST_UPLOADER(uploader)->fail_upload_retry != 0;

  TEST_UPLOADER(uploader)->upload_part_count++;

  if (ok) {
    TEST_UPLOADER(uploader)->fail_upload_retry--;
  }

  return ok;
}

static gboolean
test_uploader_complete (GstS3Uploader * uploader)
{
  return !TEST_UPLOADER(uploader)->fail_complete;
}

static GstS3UploaderClass test_uploader_class = {
  test_uploader_destroy,
  test_uploader_upload_part,
  test_uploader_complete
};

static GstS3Uploader*
test_uploader_new (gint fail_upload_retry, gboolean fail_complete)
{
  TestUploader *uploader = g_new(TestUploader, 1);

  uploader->base.klass = &test_uploader_class;
  uploader->fail_upload_retry = fail_upload_retry;
  uploader->fail_complete = fail_complete;
  uploader->upload_part_count = 0;

  return (GstS3Uploader*) uploader;
}

/************* TEST UPLOADER END *************/

static gboolean
push_bytes(GstPad *pad, size_t num_bytes, GstFlowReturn expected_ret_code)
{
  GstBuffer *buf = gst_buffer_new_and_alloc(num_bytes);
  GRand *rand = g_rand_new_with_seed (num_bytes);
  GstMapInfo info;
  guint i;
  gboolean ret = FALSE;

  if (!gst_buffer_map (buf, &info, GST_MAP_WRITE)) {
    goto buffer_map_failed;
  }

  for (i = 0; i < num_bytes; ++i)
    ((guint8 *)info.data)[i] = (g_rand_int (rand) >> 24) & 0xff;
  gst_buffer_unmap (buf, &info);

  ret = gst_pad_push (pad, buf) == expected_ret_code;
  goto push_bytes_finalize;

buffer_map_failed:
  gst_buffer_unref (buf);

push_bytes_finalize:
  g_rand_free (rand);

  return ret;
}

#define PUSH_BYTES(pad, num_bytes) fail_if (!push_bytes(pad, num_bytes, GST_FLOW_OK))
#define PUSH_BYTES_FAILURE(pad, num_bytes) fail_if (!push_bytes(pad, num_bytes, GST_FLOW_ERROR))

static GstElement *
setup_default_s3_sink (GstS3Uploader *uploader)
{
  GstElement *sink = gst_element_factory_make ("s3sink", "sink");

  if (sink) {
    g_object_set (sink,
      "bucket", "some-bucket",
      "key", "some-key",
      NULL);
    GST_S3_SINK (sink)->uploader = uploader;
  }

  return sink;
}

GST_START_TEST (test_no_bucket_and_key_then_start_should_fail)
{
  GstElement *sink = gst_element_factory_make ("s3sink", "sink");
  GstStateChangeReturn ret;

  fail_if (sink == NULL);

  ret = gst_element_set_state (sink, GST_STATE_PLAYING);
  fail_unless (ret == GST_STATE_CHANGE_FAILURE);

  gst_element_set_state (sink, GST_STATE_NULL);
  gst_object_unref (sink);
}
GST_END_TEST

GST_START_TEST (test_location_property)
{
  GstElement *sink = gst_element_factory_make ("s3sink", "sink");
  const gchar *bucket = "bucket";
  const gchar *key = "key";
  const gchar *location = "s3://bucket/key";

  gchar *returned_bucket = NULL,
    *returned_key = NULL,
    *returned_location = NULL;

  fail_if (sink == NULL);

  // Verify setting bucket and key results in location == 's3://{bucket}/{key}'
  g_object_set (sink,
    "bucket", bucket,
    "key", key,
    NULL);
  g_object_get (sink, "location", &returned_location, NULL);
  fail_if (0 != g_ascii_strcasecmp(location, returned_location));
  g_free (returned_location);
  returned_location = NULL;

  // Set the bucket and key to something different, then set location.
  // Verify bucket and key match what is expected.
  g_object_set (sink, "bucket", "", "key", "", NULL);
  g_object_set (sink, "location", location, NULL);
  g_object_get (sink, "bucket", &returned_bucket, "key", &returned_key, NULL);
  fail_if (0 != g_ascii_strcasecmp(bucket, returned_bucket));
  fail_if (0 != g_ascii_strcasecmp(key, returned_key));
  g_free (returned_bucket);
  returned_bucket = NULL;
  g_free (returned_key);
  returned_key = NULL;

  gst_object_unref (sink);
}
GST_END_TEST

GST_START_TEST (test_gst_urihandler_interface)
{
  GstElement *s3Sink = gst_element_make_from_uri(GST_URI_SINK, "s3://bucket/key", "s3sink", NULL);
  fail_if(NULL == s3Sink);
  gst_object_unref(s3Sink);
}
GST_END_TEST

GST_START_TEST (test_change_properties_after_start_should_fail)
{
  GstElement *sink = gst_element_factory_make("s3sink", "sink");
  GstStateChangeReturn ret;
  const gchar *bucket = "bucket";
  const gchar *key = "key";
  const gchar *content_type = "content-type";
  const gchar *ca_file = "/path/to/ca";
  gint buffer_size = 1024*1024*10;
  gchar *new_bucket = NULL, *new_key = NULL, *new_content_type = NULL, *new_ca_file = NULL;
  gint new_buffer_size = 0;

  fail_if (sink == NULL);

  GST_S3_SINK (sink)->uploader = test_uploader_new (-1, FALSE);

  g_object_set(sink,
    "bucket", bucket,
    "key", key,
    "content-type", content_type,
    "ca-file", ca_file,
    "buffer-size", buffer_size,
    NULL);

  ret = gst_element_set_state (sink, GST_STATE_PLAYING);
  fail_unless (ret == GST_STATE_CHANGE_ASYNC);

  g_object_set(sink,
    "bucket", "new-bucket",
    "key", "new-key",
    "content-type", "new-content-type",
    "ca-file", "new-ca-file",
    "buffer-size", buffer_size * 2,
    NULL);

  g_object_get(sink,
    "bucket", &new_bucket,
    "key", &new_key,
    "content-type", &new_content_type,
    "ca-file", &new_ca_file,
    "buffer-size", &new_buffer_size,
    NULL);

  fail_unless_equals_string(bucket, new_bucket);
  fail_unless_equals_string(key, new_key);
  fail_unless_equals_string(content_type, new_content_type);
  fail_unless_equals_string(ca_file, new_ca_file);
  fail_unless_equals_int(buffer_size, new_buffer_size);

  gst_element_set_state (sink, GST_STATE_NULL);
  gst_object_unref (sink);

  g_free(new_bucket);
  g_free(new_key);
  g_free(new_content_type);
  g_free(new_ca_file);
}
GST_END_TEST

GST_START_TEST (test_send_eos_should_flush_buffer)
{
  GstElement *sink;
  GstStateChangeReturn ret;
  GstPad *sinkpad, *srcpad;
  TestUploader *uploader = (TestUploader *) test_uploader_new (-1, FALSE);

  sink = setup_default_s3_sink ((GstS3Uploader*) uploader);
  fail_if (sink == NULL);

  srcpad = gst_check_setup_src_pad (sink, &srctemplate);
  gst_pad_set_active (srcpad, TRUE);

  ret = gst_element_set_state (sink, GST_STATE_PLAYING);
  fail_unless (ret == GST_STATE_CHANGE_ASYNC);

  PUSH_BYTES(srcpad, 10);

  sinkpad = gst_element_get_static_pad (sink, "sink");
  gst_pad_send_event(sinkpad, gst_event_new_eos ());
  gst_object_unref (sinkpad);

  fail_unless_equals_int(1, uploader->upload_part_count);

  gst_element_set_state (sink, GST_STATE_NULL);
  gst_object_unref (sink);
  gst_object_unref (srcpad);
}
GST_END_TEST

GST_START_TEST (test_push_buffer_should_flush_buffer_if_reaches_limit)
{
  GstElement *sink;
  GstStateChangeReturn ret;
  GstPad *srcpad;
  int idx;
  TestUploader *uploader = (TestUploader *) test_uploader_new (-1, FALSE);

  sink = setup_default_s3_sink ((GstS3Uploader*) uploader);
  fail_if (sink == NULL);

  g_object_set(sink, "buffer-size", 5*1024*1024, NULL);

  srcpad = gst_check_setup_src_pad (sink, &srctemplate);
  gst_pad_set_active (srcpad, TRUE);

  ret = gst_element_set_state (sink, GST_STATE_PLAYING);
  fail_unless (ret == GST_STATE_CHANGE_ASYNC);

  for (idx = 0; idx < 16; idx++) {
    PUSH_BYTES (srcpad, 1024 * 1024);
  }

  fail_unless_equals_int (3, uploader->upload_part_count);

  gst_element_set_state (sink, GST_STATE_NULL);
  gst_object_unref (sink);
  gst_object_unref (srcpad);
}
GST_END_TEST

GST_START_TEST (test_query_position)
{
  GstElement *sink = setup_default_s3_sink (test_uploader_new (-1, FALSE));
  GstStateChangeReturn ret;
  GstPad *srcpad, *sinkpad;
  const gint64 bytes_to_write = 31337;
  gint64 position_bytes = 0;

  fail_if (sink == NULL);

  srcpad = gst_check_setup_src_pad (sink, &srctemplate);
  gst_pad_set_active (srcpad, TRUE);

  sinkpad = gst_element_get_static_pad (sink, "sink");
  fail_if (sinkpad == NULL);

  ret = gst_element_set_state (sink, GST_STATE_PLAYING);
  fail_unless (ret == GST_STATE_CHANGE_ASYNC);

  PUSH_BYTES(srcpad, bytes_to_write);

  gst_pad_query_position (sinkpad, GST_FORMAT_BYTES, &position_bytes);

  fail_unless_equals_int64 (bytes_to_write, position_bytes);

  gst_element_set_state (sink, GST_STATE_NULL);
  gst_object_unref (sinkpad);
  gst_object_unref (srcpad);
  gst_object_unref (sink);
}
GST_END_TEST

GST_START_TEST (test_query_seeking)
{
  const GstFormat expected_format = GST_FORMAT_DEFAULT;
  GstFormat format;
  gboolean seekable;
  GstElement *sink = setup_default_s3_sink (test_uploader_new (-1, FALSE));
  GstStateChangeReturn ret;

  fail_if (sink == NULL);

  ret = gst_element_set_state (sink, GST_STATE_PLAYING);
  fail_unless (ret == GST_STATE_CHANGE_ASYNC);

  GstQuery* query = gst_query_new_seeking (expected_format);

  gst_element_query (sink, query);

  gst_query_parse_seeking (query, &format, &seekable, NULL, NULL);
  fail_if (seekable == TRUE);
  fail_unless (format == expected_format);

  gst_query_unref (query);

  gst_element_set_state (sink, GST_STATE_NULL);
  gst_object_unref (sink);
}
GST_END_TEST

GST_START_TEST (test_upload_part_failure)
{
  GstElement *sink = setup_default_s3_sink (test_uploader_new (2, FALSE));
  GstStateChangeReturn ret;
  GstPad *srcpad;
  const guint bytes_to_write = 5 * 1024 * 1024;

  fail_if (sink == NULL);

  g_object_set (sink, "buffer-size", bytes_to_write, NULL);

  srcpad = gst_check_setup_src_pad (sink, &srctemplate);
  gst_pad_set_active (srcpad, TRUE);

  ret = gst_element_set_state (sink, GST_STATE_PLAYING);
  fail_unless (ret == GST_STATE_CHANGE_ASYNC);

  PUSH_BYTES(srcpad, bytes_to_write);
  PUSH_BYTES(srcpad, bytes_to_write);
  PUSH_BYTES_FAILURE(srcpad, bytes_to_write);

  gst_element_set_state (sink, GST_STATE_NULL);
  gst_object_unref (srcpad);
  gst_object_unref (sink);
}
GST_END_TEST

GST_START_TEST (test_push_empty_buffer)
{
  GstElement *sink = setup_default_s3_sink (test_uploader_new (2, FALSE));
  GstStateChangeReturn ret;
  GstPad *srcpad;

  fail_if (sink == NULL);

  srcpad = gst_check_setup_src_pad (sink, &srctemplate);
  gst_pad_set_active (srcpad, TRUE);

  ret = gst_element_set_state (sink, GST_STATE_PLAYING);
  fail_unless (ret == GST_STATE_CHANGE_ASYNC);

  fail_unless_equals_int (gst_pad_push (srcpad, gst_buffer_new ()), GST_FLOW_OK);

  gst_element_set_state (sink, GST_STATE_NULL);
  gst_object_unref (srcpad);
  gst_object_unref (sink);
}
GST_END_TEST

GST_PLUGIN_STATIC_DECLARE(s3elements);

static Suite *
s3sink_suite (void)
{
  Suite *s = suite_create ("s3sink");
  TCase *tc_chain = tcase_create ("general");

  tcase_set_timeout (tc_chain, 20);

  suite_add_tcase (s, tc_chain);
  tcase_add_test (tc_chain, test_no_bucket_and_key_then_start_should_fail);
  tcase_add_test (tc_chain, test_location_property);
  tcase_add_test (tc_chain, test_gst_urihandler_interface);
  tcase_add_test (tc_chain, test_change_properties_after_start_should_fail);
  tcase_add_test (tc_chain, test_send_eos_should_flush_buffer);
  tcase_add_test (tc_chain, test_push_buffer_should_flush_buffer_if_reaches_limit);
  tcase_add_test (tc_chain, test_query_position);
  tcase_add_test (tc_chain, test_query_seeking);
  tcase_add_test (tc_chain, test_upload_part_failure);
  tcase_add_test (tc_chain, test_push_empty_buffer);

  return s;
}

GST_CHECK_MAIN (s3sink)
