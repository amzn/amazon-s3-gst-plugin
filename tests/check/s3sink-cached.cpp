#include <gst/check/gstcheck.h>

#include "gsts3sink.h"

#include "include/testcacheduploader.hpp"
#include "include/testdownloader.h"
#include "include/push_bytes.h"

static GstStaticPadTemplate srctemplate = GST_STATIC_PAD_TEMPLATE ("src",
    GST_PAD_SRC,
    GST_PAD_ALWAYS,
    GST_STATIC_CAPS_ANY);

static GstS3Uploader*
s3sink_make_new_test_uploader(const GstS3UploaderConfig *config )
{
  TestCachedUploader *uploader = (TestCachedUploader *) test_cached_uploader_new (-1, FALSE, config->cache_num_parts);
  return (GstS3Uploader*) uploader;
}

static GstS3Downloader*
s3sink_make_new_test_downloader( G_GNUC_UNUSED const GstS3UploaderConfig *config )
{
  TestDownloader *downloader = (TestDownloader *) test_downloader_new ();
  return (GstS3Downloader*) downloader;
}

#define S3SINK_UPLOADER(obj) ((TestCachedUploader*) GST_S3_SINK(obj)->uploader)
#define S3SINK_DOWNLOADER(obj) ((TestDownloader*) GST_S3_SINK(obj)->downloader)

#define PACKET_SIZE         (1024*1024)
#define PACKETS_PER_BUFFER  (5)
#define DEFAULT_BUFFER_SIZE (PACKETS_PER_BUFFER * PACKET_SIZE)

static GstElement *
setup_cached_s3sink (gint cache_limit=0)
{
  GstElement *sink =  gst_element_factory_make_full("s3sink",
    "name", "sink",
    "bucket", "some-bucket",
    "key", "some-key",
    "num-cache-parts", cache_limit,
    "buffer-size", DEFAULT_BUFFER_SIZE,
    NULL);

  GST_S3_SINK_GET_CLASS((gpointer*) sink)->uploader_new = s3sink_make_new_test_uploader;
  GST_S3_SINK_GET_CLASS((gpointer*) sink)->downloader_new = s3sink_make_new_test_downloader;
  test_cached_uploader_reset_prev_stats();

  return sink;
}

GST_START_TEST (test_head_cached)
{
  /**
   * @brief Test will run the s3sink with 1 part cached at the head (start)
   * of the upload.  It will push 12 packets (just over 2 parts), seek to
   * a few bytes into the first part, write a change, then EOS.  The expected
   * result is the seek to use the cache, and the EOS to push that cached
   * buffer then complete the upload.  There should be no need to backfill or
   * perform other kinds of operations against the S3 API.  It should also
   * not need to create a new uploader during this process.
   */
  GstStateChangeReturn ret;
  const gint num_packets = 12;

  GstElement *sink = setup_cached_s3sink(1);
  fail_if (sink == NULL);

  GstPad *srcpad = gst_check_setup_src_pad (sink, &srctemplate);
  gst_pad_set_active (srcpad, TRUE);

  ret = gst_element_set_state (sink, GST_STATE_PLAYING);
  fail_unless (ret == GST_STATE_CHANGE_ASYNC);

  fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_part_count, 0);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_copy_part_count, 0);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->cache_hits, 0);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->cache_misses, 0);
  fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->downloads_requested, 0);
  fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->bytes_downloaded, 0);

  fail_unless(prepare_to_push_bytes(srcpad, "seek_test"));
  PUSH_BYTES(srcpad, num_packets * PACKET_SIZE);

  // Should see 2 parts uploaded so far.
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_part_count, 2);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_copy_part_count, 0);

  // Seek to offset 16 in the cache.  This should cause a flush (upload),
  // so the upload part count should be 3 and a cache hit.
  GstSegment segment;
  gst_segment_init(&segment, GST_FORMAT_BYTES);
  segment.start = 16;
  gst_pad_push_event(srcpad, gst_event_new_segment(&segment));
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_part_count, 3);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->cache_hits, 1);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->cache_misses, 0);

  // Write more data.
  PUSH_BYTES(srcpad, 32);

  // EOS
  // 1. Uploader is destroyed this time as part of EOS, so
  //    checking values of prev. instead.
  // 2. upload_part_count becomes 4 (b/c wrote over part 1)
  // 3. cache_hits still 1 from above
  // 4. cache_misses = 1 (b/c part 2 is not in the cache)
  // 5. no download activity at all
  gst_pad_push_event(srcpad, gst_event_new_eos());
  fail_unless_equals_int(prev_test_cached_uploader_stats.upload_part_count, 4);
  fail_unless_equals_int(prev_test_cached_uploader_stats.cache_hits, 1);
  fail_unless_equals_int(prev_test_cached_uploader_stats.cache_misses, 1);
  fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->downloads_requested, 0);
  fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->bytes_downloaded, 0);

  // End.
  gst_element_set_state (sink, GST_STATE_NULL);
  gst_clear_object(&srcpad);
  gst_clear_object(&sink);
}
GST_END_TEST

GST_PLUGIN_STATIC_DECLARE(s3elements);

static Suite *
s3sink_cached_suite (void)
{
  Suite *s = suite_create ("s3sink_cached");
  TCase *tc_chain = tcase_create ("general");

  tcase_set_timeout (tc_chain, 30000);

  suite_add_tcase (s, tc_chain);
  tcase_add_test (tc_chain, test_head_cached);

  return s;
}

GST_CHECK_MAIN (s3sink_cached)
