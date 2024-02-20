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

#include <gst/check/gstcheck.h>

#include "gsts3uploaderpartcache.hpp"

using gst::aws::s3::UploaderPartCache;

#define TEST_BUFFER_NEW(s) (new char[s])
#define TEST_BUFFER_FREE(b) {if (b) delete [] b; b = NULL;}

GST_START_TEST(test_part_info) {
  UploaderPartCache::PartInfo uut;

  fail_unless(uut.buffer() == NULL);
  fail_unless(uut.copy_buffer() == NULL);

  auto tb = TEST_BUFFER_NEW(100);
  uut.buffer(tb, 100);

  char* out_buffer = uut.copy_buffer();
  fail_unless(out_buffer != NULL);
  fail_unless(out_buffer != uut.buffer());

  // Test clearing behavior
  fail_unless((uut.buffer()));
  uut.clear_buffer();
  fail_unless(!(uut.buffer()));
  TEST_BUFFER_FREE(out_buffer);

  // Update part info to have a null buffer, same size,
  // should result in null return.
  uut.buffer(tb, 100);
  fail_unless(uut.buffer() != NULL);
  uut.buffer(NULL, 100);
  fail_unless(uut.buffer() == NULL);

  TEST_BUFFER_FREE(tb);
}
GST_END_TEST

GST_START_TEST(test_cache_disabled)
{
  /**
   * @brief History is kept of part sizes, but buffers are not.
   */
  const int depth = 0;
  UploaderPartCache cache(depth);

  const size_t size_buffer = 100;
  char *buffer = TEST_BUFFER_NEW(size_buffer);
  char *out_buffer = NULL;
  size_t out_size_buffer = 0;
  int  out_part_num = 0;

  fail_unless(cache.size() == 0);
  fail_unless(cache.insert_or_update(1, buffer, size_buffer));
  fail_unless(cache.size() == 1);
  fail_unless(cache.find(50, &out_part_num, &out_buffer, &out_size_buffer));

  // Buffer should be NULL (not retained)
  // Size should match original.
  // Part number should be 1.
  fail_unless(out_buffer == NULL);
  fail_unless(out_size_buffer == size_buffer);
  fail_unless(out_part_num == 1);

  // 'get' should work too, same behavior as above.
  out_buffer = NULL;
  out_size_buffer = 0;
  out_part_num = 0;
  fail_unless(cache.get_copy(1, &out_buffer, &out_size_buffer));
  fail_unless(out_buffer == NULL);
  fail_unless(out_size_buffer == size_buffer);

  TEST_BUFFER_FREE(buffer);
}
GST_END_TEST

GST_START_TEST(test_find_by_offset)
{
  /**
   * @brief Push 3 100 byte parts and verify 'find' gets
   * the right parts vs. the offsets.
   *    Part 1:   0 -  99
   *    Part 2: 100 - 199
   *    Part 3: 200 - 299
   */
  const size_t buffer_size = 100;
  int offset = 0;
  UploaderPartCache cache(0);

  // Populate the cache
  for (int i = 1; i <= 3; i++) {
    fail_unless(cache.size() == i-1);
    fail_unless(cache.insert_or_update(i, NULL, buffer_size));
    fail_unless(cache.size() == i);
  }

  // Validate the cache offsets
  for (int i = 1; i <= 3; i++) {
    int out_part_num = 0;
    size_t out_buffer_size = 0;
    char *out_buffer = NULL;

    fail_unless(cache.find(offset, &out_part_num, &out_buffer, &out_buffer_size));
    fail_unless(out_part_num == i);

    fail_unless(cache.find(offset + buffer_size - 1, &out_part_num, &out_buffer, &out_buffer_size));
    fail_unless(out_part_num == i);

    offset += buffer_size;
  }
}
GST_END_TEST

GST_START_TEST(test_cache_miss)
{
  /**
   * @brief Test various failure modes for cache misses on insert/update and get.
   */
  UploaderPartCache cache(0);

  size_t buffer_size = 0;
  char *buffer = NULL;
  int part_num = 0;

  // Should not be able to find anything; nothing exists.
  fail_unless(!cache.find(20, &part_num, &buffer, &buffer_size));

  // Should not be able to get the first part, it hasn't been inserted.
  fail_unless(!cache.get_copy(1, &buffer, &buffer_size));

  // size is 0, so inserting part number 2 is invalid; this should fail.
  fail_unless(!cache.insert_or_update(2, NULL, 100));

  // Should be able to insert part 1 of 100 bytes
  fail_unless(cache.insert_or_update(1, NULL, 100));

  // Should be able to access part 1, but its buffer should be NULL, as inserted..
  fail_unless(cache.find(99, &part_num, &buffer, &buffer_size));

  // Should NOT be able to find offset 100 since that would be part 2.
  fail_unless(!cache.find(100, &part_num, &buffer, &buffer_size));
}
GST_END_TEST

GST_START_TEST(test_retain_head)
{
  /**
   * @brief History kept for first N parts, remainders are empty.
   */
  const int limit = 2;
  UploaderPartCache cache(limit);

  size_t size_buffer = 100;
  char *buffer = TEST_BUFFER_NEW(size_buffer);

  char *out_buffer = NULL;
  size_t out_size_buffer = 0;
  fail_unless(cache.size() == 0);

  for (int i = 1; i <= 3; i++) {
    char* local = NULL;
    size_t local_size = 0;

    fail_unless(cache.insert_or_update(i, buffer, size_buffer));

    // Since this is head retention, immediately upon insertion
    // if the part number is within the limit, it should be kept,
    // otherwise immediately dropped.
    if (i <= limit) {
      // Retained
      GST_TRACE("Verifying %d buffer was retained", i);
      fail_unless(cache.get_copy(i, &local, &local_size));
      fail_unless(local != NULL);
      fail_unless(local_size == size_buffer);
      GST_TRACE("going to next...");
    }
    else {
      // Immediate drop.
      GST_TRACE("Verifying %d buffer was dropped", i);
      fail_unless(cache.get_copy(i, &local, &local_size));
      fail_unless(local == NULL);
      fail_unless(local_size == size_buffer);
    }
    TEST_BUFFER_FREE(local);
  }
  fail_unless(cache.size() == 3);

  // 1 and 2 should have a buffer, 3 should not.
  fail_unless(cache.get_copy(1, &out_buffer, &out_size_buffer));
  fail_unless(out_buffer != NULL);
  fail_unless(out_size_buffer == size_buffer);
  TEST_BUFFER_FREE(out_buffer);
  out_size_buffer = 0;

  fail_unless(cache.get_copy(2, &out_buffer, &out_size_buffer));
  fail_unless(out_buffer != NULL);
  fail_unless(out_size_buffer == size_buffer);
  TEST_BUFFER_FREE(out_buffer);
  out_size_buffer = 0;

  fail_unless(cache.get_copy(3, &out_buffer, &out_size_buffer));
  fail_unless(out_buffer == NULL);
  fail_unless(out_size_buffer == size_buffer);
  out_size_buffer = 0;

  TEST_BUFFER_FREE(buffer);
}
GST_END_TEST

GST_START_TEST(test_retain_tail)
{
  /**
   * @brief History kept for last N parts, remainders are empty
   */
  UploaderPartCache cache(-2);

  size_t size_buffer = 100;
  char *buffer = TEST_BUFFER_NEW(size_buffer);

  char *out_buffer = NULL;
  size_t out_size_buffer = 0;

  fail_unless(cache.size() == 0);
  for (int i = 1; i <= 3; i++) {
    char *local = NULL;
    size_t local_size;
    fail_unless(cache.insert_or_update(i, buffer, size_buffer));

    // Since this is tail retention, the most recent part should
    // always be retained.
    fail_unless(cache.get_copy(i, &local, &local_size));
    fail_unless(local != NULL);
    fail_unless(local_size == size_buffer);
    TEST_BUFFER_FREE(local);
  }
  fail_unless(cache.size() == 3);

  // 1 should not have a buffer, 2 and 3 should
  fail_unless(cache.get_copy(1, &out_buffer, &out_size_buffer));
  fail_unless(out_buffer == NULL);
  fail_unless(out_size_buffer == size_buffer);
  out_size_buffer = 0;

  fail_unless(cache.get_copy(2, &out_buffer, &out_size_buffer));
  fail_unless(out_buffer != NULL);
  fail_unless(out_size_buffer == size_buffer);
  TEST_BUFFER_FREE(out_buffer);
  out_size_buffer = 0;

  fail_unless(cache.get_copy(3, &out_buffer, &out_size_buffer));
  fail_unless(out_buffer != NULL);
  fail_unless(out_size_buffer == size_buffer);
  TEST_BUFFER_FREE(out_buffer);
  out_size_buffer = 0;

  TEST_BUFFER_FREE(buffer);
}
GST_END_TEST


static Suite *
uploader_part_cache_suite (void)
{
  Suite *s = suite_create ("uploader_part_cache");
  TCase *tc_chain = tcase_create ("general");

  suite_add_tcase (s, tc_chain);
  tcase_add_test (tc_chain, test_part_info);
  tcase_add_test (tc_chain, test_cache_disabled);
  tcase_add_test (tc_chain, test_find_by_offset);
  tcase_add_test (tc_chain, test_cache_miss);
  tcase_add_test (tc_chain, test_retain_head);
  tcase_add_test (tc_chain, test_retain_tail);

  return s;
}

GST_CHECK_MAIN (uploader_part_cache)
