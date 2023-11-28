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
#include <gst/check/gstcheck.h>

#include "gsts3sink.h"

#include "include/push_bytes.h"
#include "include/testuploader.h"
#include "include/testdownloader.h"

static GstStaticPadTemplate srctemplate = GST_STATIC_PAD_TEMPLATE ("src",
    GST_PAD_SRC,
    GST_PAD_ALWAYS,
    GST_STATIC_CAPS_ANY);

static GstElement *
setup_default_s3_sink (GstS3Uploader *uploader, GstS3Downloader *downloader)
{
  GstElement *sink =  gst_element_factory_make_full("s3sink",
    "name", "sink",
    "bucket", "some-bucket",
    "key", "some-key",
    NULL);

  if (sink) {
    if (uploader)
      GST_S3_SINK (sink)->uploader = uploader;
    if (downloader)
      GST_S3_SINK (sink)->downloader = downloader;
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
  const gchar *location = "s3://bucket/key";

  gchar *returned_location = NULL;

  fail_if (sink == NULL);

  // Set location, then set bucket and key, verify location is used
  g_object_set (sink, "location", location, NULL);
  g_object_set (sink,
    "bucket", "new-bucket",
    "key", "new-key",
    NULL);
  g_object_get (sink, "location", &returned_location, NULL);
  fail_if (0 != g_ascii_strcasecmp(location, returned_location));
  g_free (returned_location);

  gst_object_unref (sink);
}
GST_END_TEST

GST_START_TEST (test_credentials_property)
{
  GstElement *sink = gst_element_factory_make ("s3sink", "sink");
  GstAWSCredentials *credentials = gst_aws_credentials_from_string ("access-key-id=someAccessKey|secret-access-key=someAccessSecret");

  fail_if (sink == NULL);

  g_object_set (sink,
    "aws-credentials",
    credentials,
    NULL);

  gst_aws_credentials_free (credentials);

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

  sink = setup_default_s3_sink ((GstS3Uploader*) uploader, NULL);
  fail_if (sink == NULL);

  srcpad = gst_check_setup_src_pad (sink, &srctemplate);
  gst_pad_set_active (srcpad, TRUE);

  ret = gst_element_set_state (sink, GST_STATE_PLAYING);
  fail_unless (ret == GST_STATE_CHANGE_ASYNC);

  fail_unless(TRUE == prepare_to_push_bytes(srcpad, NULL));

  PUSH_BYTES(srcpad, 10);

  sinkpad = gst_element_get_static_pad (sink, "sink");
  gst_pad_send_event(sinkpad, gst_event_new_eos ());
  gst_object_unref (sinkpad);

  fail_unless_equals_int(1, prev_test_uploader_stats.upload_part_count);

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

  sink = setup_default_s3_sink ((GstS3Uploader*) uploader, NULL);
  fail_if (sink == NULL);

  g_object_set(sink, "buffer-size", 5*1024*1024, NULL);

  srcpad = gst_check_setup_src_pad (sink, &srctemplate);
  gst_pad_set_active (srcpad, TRUE);

  ret = gst_element_set_state (sink, GST_STATE_PLAYING);
  fail_unless (ret == GST_STATE_CHANGE_ASYNC);

  fail_unless(TRUE == prepare_to_push_bytes(srcpad, NULL));

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
  GstElement *sink = setup_default_s3_sink (test_uploader_new (-1, FALSE), NULL);
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

  fail_unless(TRUE == prepare_to_push_bytes(srcpad, NULL));

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
  /**
   * @brief s3sink is seekable.  Its responses to formats should be:
   *    GST_FORMAT_DEFAULT -> GST_FORMAT_BYTES
   *    GST_FORMAT_BYTES   -> GST_FORMAT_BYTES
   *    all other formats  -> FALSE
   *
   * NOTE: the GstElement base class randomly selects a pad to query and
   * if it lands on a sink pad tries to get the peer pad for performing
   * the query.  Since the unit under test here isn't connected to a peer,
   * the query will return false after dropping the query.  So, we query
   * the sink pad directly on the element.
   */
  GstFormat expected_format;
  GstFormat format;
  gboolean seekable;
  GstElement *sink = setup_default_s3_sink (test_uploader_new (-1, FALSE), NULL);
  GstPad *sinkpad = NULL;
  GstStateChangeReturn ret;
  GstQuery *query = NULL;

  fail_if (sink == NULL);

  sinkpad = gst_element_get_static_pad(sink, "sink");
  fail_if (sinkpad == NULL);

  ret = gst_element_set_state (sink, GST_STATE_PLAYING);
  fail_unless (ret == GST_STATE_CHANGE_ASYNC);

  // default -> bytes
  expected_format = GST_FORMAT_BYTES;
  query = gst_query_new_seeking (GST_FORMAT_DEFAULT);
  gst_pad_query(sinkpad, query);
  gst_query_parse_seeking (query, &format, &seekable, NULL, NULL);
  fail_if (seekable != TRUE);
  fail_unless (format == expected_format);
  gst_query_unref (query);

  // bytes -> bytes
  expected_format = GST_FORMAT_BYTES;
  query = gst_query_new_seeking (expected_format);
  gst_pad_query(sinkpad, query);
  gst_query_parse_seeking (query, &format, &seekable, NULL, NULL);
  fail_if (seekable != TRUE);
  fail_unless (format == expected_format);
  gst_query_unref (query);

  // anything else -> nope.
  expected_format = GST_FORMAT_BUFFERS;
  query = gst_query_new_seeking (expected_format);
  gst_pad_query(sinkpad, query);
  gst_query_parse_seeking (query, &format, &seekable, NULL, NULL);
  fail_if (seekable != FALSE);
  fail_unless (format == expected_format);
  gst_query_unref (query);

  gst_element_set_state (sink, GST_STATE_NULL);
  gst_object_unref (sink);
}
GST_END_TEST

GST_START_TEST (test_upload_part_failure)
{
  GstElement *sink = setup_default_s3_sink (test_uploader_new (2, FALSE), NULL);
  GstStateChangeReturn ret;
  GstPad *srcpad;
  const guint bytes_to_write = 5 * 1024 * 1024;

  fail_if (sink == NULL);

  g_object_set (sink, "buffer-size", bytes_to_write, NULL);

  srcpad = gst_check_setup_src_pad (sink, &srctemplate);
  gst_pad_set_active (srcpad, TRUE);

  ret = gst_element_set_state (sink, GST_STATE_PLAYING);
  fail_unless (ret == GST_STATE_CHANGE_ASYNC);

  fail_unless(TRUE == prepare_to_push_bytes(srcpad, NULL));

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
  GstElement *sink = setup_default_s3_sink (test_uploader_new (2, FALSE), NULL);
  GstStateChangeReturn ret;
  GstPad *srcpad;

  fail_if (sink == NULL);

  srcpad = gst_check_setup_src_pad (sink, &srctemplate);
  gst_pad_set_active (srcpad, TRUE);

  ret = gst_element_set_state (sink, GST_STATE_PLAYING);
  fail_unless (ret == GST_STATE_CHANGE_ASYNC);

  fail_unless(TRUE == prepare_to_push_bytes(srcpad, NULL));

  fail_unless_equals_int (gst_pad_push (srcpad, gst_buffer_new ()), GST_FLOW_OK);

  gst_element_set_state (sink, GST_STATE_NULL);
  gst_object_unref (srcpad);
  gst_object_unref (sink);
}
GST_END_TEST

static GstS3Uploader*
s3sink_make_new_test_uploader( G_GNUC_UNUSED const GstS3UploaderConfig *config )
{
  TestUploader *uploader = (TestUploader *) test_uploader_new (-1, FALSE);
  return (GstS3Uploader*) uploader;
}

static GstS3Downloader*
s3sink_make_new_test_downloader( G_GNUC_UNUSED const GstS3UploaderConfig *config )
{
  TestDownloader *downloader = (TestDownloader *) test_downloader_new ();
  return (GstS3Downloader*) downloader;
}

#define S3SINK_UPLOADER(obj) ((TestUploader*) GST_S3_SINK(obj)->uploader)
#define S3SINK_DOWNLOADER(obj) ((TestDownloader*) GST_S3_SINK(obj)->downloader)

GST_START_TEST (test_seek_to_active_part)
{
  /**
   * This test verifies the case where several parts are pushed and a seek
   * request moves the offset to within the part being filled.  This kind
   * of seek should not destroy the uploader or cause any download, back-fill,
   * or other types of behaviors since the buffer is still in memory at the
   * uploader instance.
   */
  GstElement *sink = NULL;
  GstPad *srcpad = NULL;
  GstStateChangeReturn ret;
  guint packets_per_buffer = 5;
  guint packet_size = 1024 * 1024;
  guint buffer_size = packets_per_buffer * packet_size;

  // Setup the test apparatus so the s3sink calls our constructors for the
  // uploader and downloader instances.
  sink = setup_default_s3_sink (NULL, NULL);
  fail_if (sink == NULL);
  g_object_set(sink, "buffer-size", buffer_size, NULL);
  GST_S3_SINK_GET_CLASS((gpointer*) sink)->uploader_new = s3sink_make_new_test_uploader;
  GST_S3_SINK_GET_CLASS((gpointer*) sink)->downloader_new = s3sink_make_new_test_downloader;

  srcpad = gst_check_setup_src_pad (sink, &srctemplate);
  gst_pad_set_active (srcpad, TRUE);

  ret = gst_element_set_state (sink, GST_STATE_PLAYING);
  fail_unless (ret == GST_STATE_CHANGE_ASYNC);

  // Push a few packets over two buffers worth of data
  // The uploader should show a count of 2.
  // Internally, the s3sink will have created the third
  // buffer and began filling it.
  prepare_to_push_bytes(srcpad, "seek_test");
  PUSH_BYTES(srcpad, buffer_size * 2 + packet_size * 3);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_part_count, 2);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_copy_part_count, 0);

  // Seek back to within that last part
  {
    GstSegment segment;
    gst_segment_init(&segment, GST_FORMAT_BYTES);
    segment.start = buffer_size * 2 + packet_size;
    gst_pad_push_event(srcpad, gst_event_new_segment(&segment));

    // 'uploader' is not trashed; read from it directly that the uploaded
    // parts is still 2.
    fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_part_count, 2);
    fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_copy_part_count, 0);

    // There should be no downloads so far.
    fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->downloads_requested, 0);
    fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->bytes_downloaded, 0);
  }

  // Push the change of this example, a packet
  // Again, no upload/download activity is expected since we're still within
  // the same third buffer.
  PUSH_BYTES(srcpad, packet_size);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_part_count, 2);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_copy_part_count, 0);
  fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->downloads_requested, 0);
  fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->bytes_downloaded, 0);

  // send eos.
  // This should cause the last buffer to flush (upload) which will destroy
  // the uploader, so we check the 'prev' stats for a value of 3.  No downloads
  // should have happened.
  gst_pad_push_event(srcpad, gst_event_new_eos());
  fail_unless_equals_int(prev_test_uploader_stats.upload_part_count, 3);
  fail_unless_equals_int(prev_test_uploader_stats.upload_copy_part_count, 0);
  fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->downloads_requested, 0);
  fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->bytes_downloaded, 0);

  test_uploader_reset_prev_stats();
  gst_element_set_state (sink, GST_STATE_NULL);
  gst_clear_object(&srcpad);
  gst_clear_object(&sink);
}
GST_END_TEST

GST_START_TEST (test_seek_to_middle_part_short)
{
  /**
   * This test verifies the case where several parts are pushed and a seek
   * request moves the offset to within a previously uploaded part (but not
   * the first part).  In this case we expect:
   * 1. The uploader to flush (complete) the active buffer
   * 2. Copy-upload the previous part(s) from 0->offset
   *    (log will say "using multipart upload copy")
   * 3. Any data pushed thereafter will be treated as per the usual
   * 4. EOS will copy-upload any previously uploaded data not including
   *    what was pushed in (3).
   *
   * NOTE: This test edits the second part with a partially filled third
   *       part.  This means the last part is downloaded then re-uploaded.
   */
  GstElement *sink = NULL;
  GstPad *srcpad = NULL;
  GstStateChangeReturn ret;
  guint packets_per_buffer = 5;
  guint packet_size = 1024 * 1024;
  guint buffer_size = packets_per_buffer * packet_size;
  guint total_size = buffer_size * 2 + packet_size * 3;

  // Setup the test apparatus so the s3sink calls our constructors for the
  // uploader and downloader instances.
  sink = setup_default_s3_sink (NULL, NULL);
  fail_if (sink == NULL);
  g_object_set(sink, "buffer-size", buffer_size, NULL);
  GST_S3_SINK_GET_CLASS((gpointer*) sink)->uploader_new = s3sink_make_new_test_uploader;
  GST_S3_SINK_GET_CLASS((gpointer*) sink)->downloader_new = s3sink_make_new_test_downloader;

  srcpad = gst_check_setup_src_pad (sink, &srctemplate);
  gst_pad_set_active (srcpad, TRUE);

  ret = gst_element_set_state (sink, GST_STATE_PLAYING);
  fail_unless (ret == GST_STATE_CHANGE_ASYNC);

  // Push a few packets over two buffers worth of data
  // The uploader should show a count of 2.
  // Internally, the s3sink will have created the third
  // buffer and began filling it.
  prepare_to_push_bytes(srcpad, "seek_test");
  PUSH_BYTES(srcpad, total_size);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_part_count, 2);

  // Seek back to the middle somewhere, causing the buffer to flush,
  // uploader to be destroyed, and a download from 0->(segment.start)-1
  // (the download is via the copy-upload API).
  {
    GstSegment segment;
    gst_segment_init(&segment, GST_FORMAT_BYTES);
    segment.start = buffer_size + packet_size + 16;
    gst_pad_push_event(srcpad, gst_event_new_segment(&segment));

    // 'uploader' destroyed after uploading 3rd part.
    fail_unless_equals_int(prev_test_uploader_stats.upload_part_count, 3);

    // seek should have caused a new uploader to be created and an upload copy event
    // to buffer up the 0->segment.start-1 portion from s3.
    fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_part_count, 0);
    fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_copy_part_count, 1);

    // 'downloader' shows no activity; this was a copy-upload event
    fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->downloads_requested, 0);
    fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->bytes_downloaded, 0);
  }

  // Push the change
  // No changes to uploader or downloader are expected
  PUSH_BYTES(srcpad, 16);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_part_count, 0);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_copy_part_count, 1);
  fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->downloads_requested, 0);

  // send eos.
  // This should cause:
  // 1. Destruction of uploader (use 'prev' stats for verification)
  // 2. Download (post-fill) from current position to 1 full part.
  //    post_fill = buffer_size - 16
  // 3. Upload #2 (upload part)
  // 4. Re-upload last /remaining partial part (download + upload part)
  //    size is end of post-fill to total size, so:
  //    last_part = total_size - current_pos
  //              = total_size - (seek_pos + write_size + post_fill)
  //              = total_size - (buffer_size + packet_size + 16 + 16 + buffer_size - 16)
  //    bytes_downloaded
  //      = post_fill + last_part
  //      = (buffer_size - 16) + (total_size - (buffer_size + packet_size + 16 + 16 + buffer_size - 16))
  //      = buffer_size - 16 + total_size - buffer_size - packet_size - 16 - 16 - buffer_size + 16
  //      = total_size - buffer_size - packet_size - 16 - 16
  gst_pad_push_event(srcpad, gst_event_new_eos());
  fail_unless_equals_int(prev_test_uploader_stats.upload_part_count, 2);
  fail_unless_equals_int(prev_test_uploader_stats.upload_copy_part_count, 1);
  fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->downloads_requested, 2);
  fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->bytes_downloaded,
    total_size - buffer_size - packet_size - 16 - 16);

  test_uploader_reset_prev_stats();
  gst_element_set_state (sink, GST_STATE_NULL);
  gst_clear_object(&srcpad);
  gst_clear_object(&sink);
}
GST_END_TEST

GST_START_TEST (test_seek_to_middle_part_long)
{
  /**
   * This test verifies the case where several parts are pushed and a seek
   * request moves the offset to within a part greater than a buffer length
   * away from the end of the already-uploaded file.
   * 1. The uploader to flush (complete) the active buffer
   * 2. Copy-upload the previous part(s) from 0->offset
   *    (log will say "using multipart upload copy")
   * 3. Any data pushed thereafter will be treated as per the usual
   * 4. EOS will copy-upload any previously uploaded data not including
   *    what was pushed in (3).
   *
   * NOTE: This test edits the second part with a partially filled fourth
   *       part.  This means the last parts are copy-uploaded.
   */
  GstElement *sink = NULL;
  GstPad *srcpad = NULL;
  GstStateChangeReturn ret;
  guint packets_per_buffer = 5;
  guint packet_size = 1024 * 1024;
  guint buffer_size = packets_per_buffer * packet_size;
  guint total_size = buffer_size * 3 +  packet_size * 3;

  // Setup the test apparatus so the s3sink calls our constructors for the
  // uploader and downloader instances.
  sink = setup_default_s3_sink (NULL, NULL);
  fail_if (sink == NULL);
  g_object_set(sink, "buffer-size", buffer_size, NULL);
  GST_S3_SINK_GET_CLASS((gpointer*) sink)->uploader_new = s3sink_make_new_test_uploader;
  GST_S3_SINK_GET_CLASS((gpointer*) sink)->downloader_new = s3sink_make_new_test_downloader;

  srcpad = gst_check_setup_src_pad (sink, &srctemplate);
  gst_pad_set_active (srcpad, TRUE);

  ret = gst_element_set_state (sink, GST_STATE_PLAYING);
  fail_unless (ret == GST_STATE_CHANGE_ASYNC);

  // Push a few packets over two buffers worth of data
  // The uploader should show a count of 3.
  // Internally, the s3sink will have created the third
  // buffer and began filling it.
  prepare_to_push_bytes(srcpad, "seek_test");
  PUSH_BYTES(srcpad, total_size);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_part_count, 3);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_copy_part_count, 0);

  // Seek back into part 2, causing a flush, uploader to be destroyed,
  // and a copy-upload from 0->(segment.start)-1
  {
    GstSegment segment;
    gst_segment_init(&segment, GST_FORMAT_BYTES);
    segment.start = buffer_size + packet_size + 16;
    gst_pad_push_event(srcpad, gst_event_new_segment(&segment));

    // 'uploader' destroyed after uploading 4th part.
    fail_unless_equals_int(prev_test_uploader_stats.upload_part_count, 4);

    // seek should have caused a new uploader to be created and an upload copy event
    // to buffer up the 0->segment.start-1 portion from s3.
    fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_part_count, 0);
    fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_copy_part_count, 1);

    // 'downloader' shows no activity; this was a copy-upload event
    fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->downloads_requested, 0);
    fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->bytes_downloaded, 0);
  }

  // Push the change
  // No changes to uploader or downloader are expected
  PUSH_BYTES(srcpad, 16);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_part_count, 0);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_copy_part_count, 1);
  fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->downloads_requested, 0);

  // send eos.
  // This should cause:
  // 1. Destruction of uploader (use 'prev' stats for verification)
  // 2. Download (post-fill) second part until it's a full part.
  //    post_fill = buffer_size - 16
  // 3. Upload #2 (upload part)
  // 4. Copy-upload parts 3 and 4 (this is the second copy-upload after the seek
  //    using a copy-upload to move the write head)
  //
  // bytes_downloaded = post_fill
  gst_pad_push_event(srcpad, gst_event_new_eos());
  fail_unless_equals_int(prev_test_uploader_stats.upload_part_count, 1);
  fail_unless_equals_int(prev_test_uploader_stats.upload_copy_part_count, 2);
  fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->downloads_requested, 1);
  fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->bytes_downloaded,
    buffer_size - 16);

  test_uploader_reset_prev_stats();
  gst_element_set_state (sink, GST_STATE_NULL);
  gst_clear_object(&srcpad);
  gst_clear_object(&sink);
}
GST_END_TEST

GST_START_TEST (test_seek_to_first_part)
{
  /**
   * The purpose of this test is to verify correct behavior when an upstream
   * element seeks back to the start, re-writes some amount of data, and then
   * sends an EOS.  Per other tests, on EOS, the element is to flush its buffer,
   * so if that pre-EOS push was already sent, then the "flush" should not
   * result in any change to data.
   *
   * This test is specifically related to the mp4mux element but may apply to
   * others.  The typical event pattern looks like this:
   * 1. Several buffers have been written,a partial buffer is filling
   * 2. A segment->start = 32 (offset 32 bytes) arrives, seek to offset 32.
   * 3. s3sink uploads the partial buffer ("Uploading X byte part", uploader destoryed)
   * 4. s3sink performs the seek ("Seeking to offset 32 by downloading") which loads 0->offset into 'buffer'
   * 5. s3sink uploader instance created
   * 6. Some data is written (16 bytes)
   * 7. EOS arrives, causing a flush into the uploader
   * 8. s3sink "post fills" the current buffer range from 48-5Mbytes
   *    by downloading that portion and flushing it into the uploader
   * 9. s3sink then copy-uploads the remaining file over itself (uploader destroyed)
   */
  GstElement *sink = NULL;
  GstPad *srcpad = NULL;
  GstStateChangeReturn ret;
  guint packets_per_buffer = 5;
  guint packet_size = 1024 * 1024;
  guint buffer_size = packets_per_buffer * packet_size;
  guint header_size = 16;

  // Setup the test apparatus so the s3sink calls our constructors for the
  // uploader and downloader instances.
  sink = setup_default_s3_sink (NULL, NULL);
  fail_if (sink == NULL);
  g_object_set(sink, "buffer-size", buffer_size, NULL);
  GST_S3_SINK_GET_CLASS((gpointer*) sink)->uploader_new = s3sink_make_new_test_uploader;
  GST_S3_SINK_GET_CLASS((gpointer*) sink)->downloader_new = s3sink_make_new_test_downloader;

  srcpad = gst_check_setup_src_pad (sink, &srctemplate);
  gst_pad_set_active (srcpad, TRUE);

  ret = gst_element_set_state (sink, GST_STATE_PLAYING);
  fail_unless (ret == GST_STATE_CHANGE_ASYNC);

  // Push a few packets over two buffers worth of data
  // The uploader should show a count of 2.
  // Internally, the s3sink will have created the third
  // buffer and began filling it.
  prepare_to_push_bytes(srcpad, "seek_test");
  PUSH_BYTES(srcpad, buffer_size * 2 + packet_size * 3);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_part_count, 2);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_copy_part_count, 0);

  // Seek back to beginning (32 bytes offset).  There's a partial
  // buffer present so this causes a flush (write) of that buffer
  // to s3 and then completing the uploader so the object is valid
  // on s3.  Then, a download happens for the impacted 'part' and
  // the buffer position pointers are moved accordingly.
  {
    GstSegment segment;
    gst_segment_init(&segment, GST_FORMAT_BYTES);
    segment.start = 32;
    gst_pad_push_event(srcpad, gst_event_new_segment(&segment));

    // 'uploader' is now trashed so check the stats it left to
    // verify it's now 3.
    fail_unless_equals_int(prev_test_uploader_stats.upload_part_count, 3);
    fail_unless_equals_int(prev_test_uploader_stats.upload_copy_part_count, 0);

    // There should be 1 download and a full buffer requested.
    fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->downloads_requested, 1);
    fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->bytes_downloaded, segment.start);
  }

  // Push the header change of this example, 16 bytes.
  PUSH_BYTES(srcpad, header_size);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_part_count, 0);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_copy_part_count, 0);

  // send eos.
  // This will cause the buffer being written to be backfilled from S3 with the
  // already-written data followed by a copy-upload of the remaining data.
  // This is 2 operations, so the upload_part_count will be 1 and the
  // upload_copy_part_count will be 1.  We're checking the 'prev' count because
  // the uploader has been destroyed by this operation.
  // The downloader will indicate the 'post fill' data has also been downloaded,
  // increasing the download count to 2 and the amount pulled to be a full buffer
  // minus the header write.
  gst_pad_push_event(srcpad, gst_event_new_eos());
  fail_unless_equals_int(prev_test_uploader_stats.upload_part_count, 1);
  fail_unless_equals_int(prev_test_uploader_stats.upload_copy_part_count, 1);
  fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->downloads_requested, 2);
  fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->bytes_downloaded, buffer_size - header_size);

  test_uploader_reset_prev_stats();
  gst_element_set_state (sink, GST_STATE_NULL);
  gst_clear_object(&srcpad);
  gst_clear_object(&sink);
}
GST_END_TEST

GST_START_TEST (test_retry_max_scale)
{
  // Verify that configuring the retry max+scale properties before tansitioning out of NULL
  // works by verifying it no longer takes ~25 seconds to fail the connection when using the
  // real downloader.
  // In the default case, the attempts are the sum of delays:
  //   ~25550 ms = 0, 50, 100, 200, 400, 800, 1600, 3200, 6400, 12800 ms.
  // Configuring then to 3 retries, the total expected delay is:
  //      150 ms = 0, 50, 100
  gint64 started = 0;
  gint64 stopped = 0;
  gfloat startup_time = 0;
  GstStateChangeReturn scr = GST_STATE_CHANGE_SUCCESS;
  gfloat diff_ms = 0;

  GstElement *sink = gst_element_factory_make_full ("s3sink",
    "name", "sink",
    "region", "bad-region",
    "bucket", "two-tears-inna",
    "key", "some-key",
    "aws-sdk-request-timeout", 2000,
    "aws-sdk-retry-max", 0,
    NULL);

  started = g_get_monotonic_time ();
  scr = gst_element_set_state (sink, GST_STATE_READY);
  stopped = g_get_monotonic_time ();
  startup_time = ((gfloat)(stopped - started)) / 1000.0f;

  GST_INFO ("Startup time: %f ms", startup_time);

  g_object_set (sink,
    "aws-sdk-retry-max", 3,
    "aws-sdk-retry-scale", 25,
    NULL);

  started = g_get_monotonic_time ();
  scr = gst_element_set_state (sink, GST_STATE_READY);
  stopped = g_get_monotonic_time ();
  fail_unless (scr == GST_STATE_CHANGE_FAILURE);
  diff_ms = ((gfloat)(stopped - started)) / 1000.0f;

  GST_INFO ("State change returned %d, took %f ms", scr, diff_ms);

  fail_if ((diff_ms - startup_time) > 300);
  fail_if ((diff_ms - startup_time * 0.75f) < 150);
  gst_element_set_state (sink, GST_STATE_NULL);
  gst_clear_object (&sink);
}
GST_END_TEST

GST_PLUGIN_STATIC_DECLARE(s3elements);

static Suite *
s3sink_suite (void)
{
  Suite *s = suite_create ("s3sink");
  TCase *tc_chain = tcase_create ("general");

  tcase_set_timeout (tc_chain, 30);

  suite_add_tcase (s, tc_chain);
  tcase_add_test (tc_chain, test_no_bucket_and_key_then_start_should_fail);
  tcase_add_test (tc_chain, test_location_property);
  tcase_add_test (tc_chain, test_credentials_property);
  tcase_add_test (tc_chain, test_gst_urihandler_interface);
  tcase_add_test (tc_chain, test_change_properties_after_start_should_fail);
  tcase_add_test (tc_chain, test_send_eos_should_flush_buffer);
  tcase_add_test (tc_chain, test_push_buffer_should_flush_buffer_if_reaches_limit);
  tcase_add_test (tc_chain, test_query_position);
  tcase_add_test (tc_chain, test_query_seeking);
  tcase_add_test (tc_chain, test_upload_part_failure);
  tcase_add_test (tc_chain, test_push_empty_buffer);
  tcase_add_test (tc_chain, test_seek_to_active_part);
  tcase_add_test (tc_chain, test_seek_to_middle_part_short);
  tcase_add_test (tc_chain, test_seek_to_middle_part_long);
  tcase_add_test (tc_chain, test_seek_to_first_part);
  tcase_add_test (tc_chain, test_retry_max_scale);

  return s;
}

GST_CHECK_MAIN (s3sink)
