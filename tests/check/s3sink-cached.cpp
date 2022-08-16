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
 * The focus of these tests are on the caching mechanism (head, tail)
 * and the resulting fidelity of the uploaded buffer as the s3sink
 * and its multipart uploader try to make use of locally-cached
 * copies of previously-uploaded parts.
 */
#include "gsts3sink.h"

#include "include/testcachedloaders.hpp"
#include "include/push_bytes.h"

#include <gst/check/gstcheck.h>

static GstStaticPadTemplate srctemplate = GST_STATIC_PAD_TEMPLATE ("src",
    GST_PAD_SRC,
    GST_PAD_ALWAYS,
    GST_STATIC_CAPS_ANY);

static GstS3Uploader*
s3sink_make_new_test_uploader(const GstS3UploaderConfig *config )
{
  return test_cached_uploader_new (config);
}

static GstS3Downloader*
s3sink_make_new_test_downloader( G_GNUC_UNUSED const GstS3UploaderConfig *config )
{
  return test_cached_downloader_new (config);
}

#define S3SINK_UPLOADER(obj) ((TestCachedUploader*) GST_S3_SINK(obj)->uploader)
#define S3SINK_DOWNLOADER(obj) ((TestCachedDownloader*) GST_S3_SINK(obj)->downloader)

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
  for (int i = 1; i <= num_packets; i++) {
    PUSH_VAL_BYTES(srcpad, PACKET_SIZE, i);
  }

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
  PUSH_VAL_BYTES(srcpad, 32, 0xFF);

  // EOS
  // 1. Uploader is destroyed this time as part of EOS, so
  //    checking values of prev. instead.
  // 2. upload_part_count becomes 4 (b/c wrote over part 1)
  // 3. cache_hits still 1 from above
  // 4. cache_misses = 2:
  //    a. part 2 was not in cache on the flush (step 2)
  //    b. the deliberate miss on EOS to see if the uploader knows the EOF
  //       so it can avoid doing any other API ops.
  // 5. no download activity at all
  gst_pad_push_event(srcpad, gst_event_new_eos());
  fail_unless_equals_int(prev_test_cached_uploader_stats.upload_part_count, 4);
  fail_unless_equals_int(prev_test_cached_uploader_stats.cache_hits, 1);
  fail_unless_equals_int(prev_test_cached_uploader_stats.cache_misses, 2);
  fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->downloads_requested, 0);
  fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->bytes_downloaded, 0);

  // Fidelity spot check:
  size_t aoi_first = segment.start;
  size_t aoi_last = aoi_first + 32 - 1;
  fail_unless_equals_int (uploaded_buffer.size(), PACKET_SIZE * num_packets);
  fail_unless_equals_int_hex ((0xFF & uploaded_buffer[aoi_first-1]),   0x01);
  fail_unless_equals_int_hex ((0xFF & uploaded_buffer[aoi_first]),     0xFF); // header change start
  fail_unless_equals_int_hex ((0xFF & uploaded_buffer[aoi_last]),      0xFF); // header change end
  fail_unless_equals_int_hex ((0xFF & uploaded_buffer[aoi_last+1]),    0x01);
  fail_unless_equals_int_hex ((0xFF & uploaded_buffer[PACKET_SIZE]),   0x02);
  fail_unless_equals_int_hex ((0xFF & uploaded_buffer[PACKET_SIZE*2]), 0x03);
  fail_unless_equals_int_hex ((0xFF & uploaded_buffer[PACKET_SIZE*3]), 0x04);
  fail_unless_equals_int_hex ((0xFF & uploaded_buffer[PACKET_SIZE*9]), 0x0A); // and so forth
  fail_unless_equals_int_hex ((0xFF & uploaded_buffer.back()),         0x0C); // EOF, expect 12 -> 0xC

  // End.
  gst_element_set_state (sink, GST_STATE_NULL);
  gst_clear_object(&srcpad);
  gst_clear_object(&sink);
  test_cached_uploader_reset_prev_stats();
}
GST_END_TEST

GST_START_TEST (test_cache_miss_on_part_boundary)
{
  /**
   * @brief This test verifies the behavior where a seek back into the
   * cache is successful but continued writes eventually run out of
   * the cache and cause an older-style seek to download the next buffer.
   * The s3sink configuration will be to cache 2 buffers.  The test will
   * write just over 3 buffers (parts).  Then a seek into the boundary
   * just before parts 2 and 3 will be performed.  Next, a write that
   * crosses this boundary.  Finally, EOS.
   *
   * The s3sink should:
   * 1. Non-destructive flush on 'seek' into part 2.
   * 2. Retrieve part 2 from local cache.
   * 3. Write results in standard "seek" behavior when crossing the 2-3
   *    part boundary (i.e., destructive seek, uploader instance destroyed)
   * 4. On EOS:
   *    a. 'backfill' (download) the remaining part 3.
   *    b. flush (upload) part 3
   *    c. detect part 4 is already on s3 and perform a 'soft seek'
   *       to quit.
   *    d. Destroy uploader
   */
  GstStateChangeReturn ret;
  const size_t total_size = 3 * DEFAULT_BUFFER_SIZE + PACKET_SIZE;
  const size_t seek_offset = (2 * DEFAULT_BUFFER_SIZE) - 10;
  const size_t write_size = 20; // bytes
  GstElement *sink = setup_cached_s3sink(2);

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
  PUSH_VAL_BYTES(srcpad, total_size, 0x1);

  // Should see 3 full parts uploaded so far.
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_part_count, 3);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_copy_part_count, 0);

  // Seek to end of part 2, witness:
  // 1. partial part 4 uploaded during flush before seek
  // 2. seek via cache (cache hit)
  GstSegment segment;
  gst_segment_init(&segment, GST_FORMAT_BYTES);
  segment.start = seek_offset;
  gst_pad_push_event(srcpad, gst_event_new_segment(&segment));
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_part_count, 4);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->cache_hits, 1);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->cache_misses, 0);

  // Write bytes.
  // .... part 2 -10 ] [ +9 part 3   ]
  //              ^-------^ write_size bytes
  PUSH_VAL_BYTES(srcpad, write_size, 0xFF);

  // Results:
  // 1. part 2 re-uploaded (part count == 5) on flush
  // 2. cache miss for part 3, but it's the head of that part, so we continue writing.
  // 3. No downloads requested yet.
  // 4. This write across the boundary made the uploader 'dirty'.
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->upload_part_count, 5);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->cache_hits, 1);
  fail_unless_equals_int(S3SINK_UPLOADER(sink)->cache_misses, 1);
  fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->downloads_requested, 0);
  fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->bytes_downloaded, 0);

  // Send EOS
  // 1. uploader is recreated (it's 'dirty') so that part 3 can be post-filled;
  //    this involves a copy-upload (+1)
  // 2. Post-fill part 3 (download +1)
  // 3. Flush part 3 (upload +1, cache miss to part 4 (miss +1))
  // 4. Remaining part is less than a full part, so download (+1) then
  //    flush (upload +1)
  gst_pad_push_event(srcpad, gst_event_new_eos());
  fail_unless_equals_int(prev_test_cached_uploader_stats.upload_copy_part_count, 1);
  fail_unless_equals_int(prev_test_cached_uploader_stats.upload_part_count, 2);
  fail_unless_equals_int(prev_test_cached_uploader_stats.cache_hits, 0);
  fail_unless_equals_int(prev_test_cached_uploader_stats.cache_misses, 1);
  fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->downloads_requested, 2);
  fail_unless_equals_int(S3SINK_DOWNLOADER(sink)->bytes_downloaded,
    DEFAULT_BUFFER_SIZE - 10 + // tail of part 3
    + PACKET_SIZE              // part 4
  );

  // Fidelity spot check around the patched area
  // Outside the patched range is 1's, inside is FF's
  size_t aoi_first = seek_offset;
  size_t aoi_last = aoi_first + write_size - 1;
  fail_unless_equals_int (uploaded_buffer.size(), total_size);
  fail_unless_equals_int_hex ((0xFF & uploaded_buffer[aoi_first - 1]), 0x01);
  fail_unless_equals_int_hex ((0xFF & uploaded_buffer[aoi_first]),     0xFF); // patch start
  fail_unless_equals_int_hex ((0xFF & uploaded_buffer[aoi_last]),      0xFF); // patch end
  fail_unless_equals_int_hex ((0xFF & uploaded_buffer[aoi_last + 1]),  0x01);
  fail_unless_equals_int_hex ((0xFF & uploaded_buffer.back()),         0x01); // EOF

  // End.
  gst_element_set_state (sink, GST_STATE_NULL);
  gst_clear_object(&srcpad);
  gst_clear_object(&sink);
  test_cached_uploader_reset_prev_stats();
}
GST_END_TEST

GST_PLUGIN_STATIC_DECLARE(s3elements);

static Suite *
s3sink_cached_suite (void)
{
  Suite *s = suite_create ("s3sink_cached");
  TCase *tc_chain = tcase_create ("general");

  tcase_set_timeout (tc_chain, 30);

  suite_add_tcase (s, tc_chain);
  tcase_add_test (tc_chain, test_head_cached);
  tcase_add_test (tc_chain, test_cache_miss_on_part_boundary);

  return s;
}

GST_CHECK_MAIN (s3sink_cached)
