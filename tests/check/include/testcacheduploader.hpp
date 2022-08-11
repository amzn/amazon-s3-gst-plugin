#pragma once

#include <sstream>
#include <gst/gst.h>
#include "gsts3uploader.h"
#include "gsts3uploaderconfig.h"

#include <gsts3uploaderpartcache.hpp>

using gst::aws::s3::UploaderPartCache;

typedef struct {
    GstS3Uploader base;

    UploaderPartCache *cache;

    gint upload_part_count;
    gint upload_copy_part_count;
    gint cache_hits;
    gint cache_misses;
    gsize buffer_size;
} TestCachedUploader;

typedef struct {
  gint upload_part_count;
  gint upload_copy_part_count;
  gint cache_hits;
  gint cache_misses;
} TestCachedUploaderStats;

static TestCachedUploaderStats prev_test_cached_uploader_stats;

// cannot put this by value in our glib-created TestCachedUploader
// as that causes a segfault
std::stringstream uploaded_buffer;

static gsize
uploaded_buffer_size() {
  gsize current = uploaded_buffer.tellp();

  uploaded_buffer.seekp(0, std::ios::end);
  gsize size = uploaded_buffer.tellp();
  uploaded_buffer.seekp(current, std::ios::beg);

  return size;
}

static void
debug_print_uploaded_buffer(size_t first, size_t last) {
  first = MIN(0, first);
  last = MAX(last, uploaded_buffer_size());

  std::string now = uploaded_buffer.str();

  for (auto i = first; i < last; i++) {
    GST_TRACE("uploaded buffer offset %010ld -> %d", i, now[i]);
  }
}

#define TEST_CACHED_UPLOADER(uploader) ((TestCachedUploader*) uploader)

static void
test_cached_uploader_reset_prev_stats()
{
  prev_test_cached_uploader_stats.upload_part_count = 0;
  prev_test_cached_uploader_stats.upload_copy_part_count = 0;
  prev_test_cached_uploader_stats.cache_hits = 0;
  prev_test_cached_uploader_stats.cache_misses = 0;
  uploaded_buffer.clear();
}

static void
test_cached_uploader_destroy (GstS3Uploader * uploader)
{
  TestCachedUploader *inst = TEST_CACHED_UPLOADER(uploader);
  prev_test_cached_uploader_stats.upload_part_count = inst->upload_part_count;
  prev_test_cached_uploader_stats.upload_copy_part_count = inst->upload_copy_part_count;
  prev_test_cached_uploader_stats.cache_hits = inst->cache_hits;
  prev_test_cached_uploader_stats.cache_misses = inst->cache_misses;

  delete inst->cache;
  inst->cache = NULL;
  g_free(uploader);
}

static gboolean
test_cached_uploader_upload_part (
  GstS3Uploader * uploader,
  const gchar * buffer,
  gsize size,
  gchar **next,
  gsize *next_size)
{
  TestCachedUploader *inst = TEST_CACHED_UPLOADER(uploader);

  inst->upload_part_count++;

  // add this buffer (part) to the uploaded_buffer.
  uploaded_buffer.write(buffer, size);

  inst->cache->get_copy(inst->upload_part_count+1, next, next_size);
  if (*next != NULL)
    inst->cache_hits++;
  else if (*next_size > 0)
    inst->cache_misses++;
  inst->cache->insert_or_update(inst->upload_part_count, buffer, size);

  return TRUE;
}

static gboolean
test_cached_uploader_upload_part_copy (GstS3Uploader * uploader, G_GNUC_UNUSED const gchar * bucket,
  G_GNUC_UNUSED const gchar * key, G_GNUC_UNUSED gsize first, G_GNUC_UNUSED gsize last)
{
  TEST_CACHED_UPLOADER(uploader)->upload_copy_part_count++;
  return TRUE;
}

static gboolean
test_cached_uploader_seek (GstS3Uploader *uploader, gsize offset, gchar **buffer, gsize *_size)
{
  TestCachedUploader *inst = TEST_CACHED_UPLOADER(uploader);

  gint part;
  if (inst->cache->find(offset, &part, buffer, _size)) {
    if (*buffer != NULL) {
      inst->cache_hits++;

      // the seek is relative to the part
      // assuming all parts are the same size
      gsize boffset = (part-1) * inst->buffer_size;
      uploaded_buffer.seekp (boffset, std::ios::beg);
      return TRUE;
    }
    else {
      inst->cache_misses++;
    }
  }
  else {
    inst->cache_misses++;
  }
  return FALSE;
}

static gboolean
test_cached_uploader_complete (GstS3Uploader * uploader)
{
  return TRUE;
}

static GstS3UploaderClass test_cached_uploader_class = {
  test_cached_uploader_destroy,
  test_cached_uploader_upload_part,
  test_cached_uploader_upload_part_copy,
  test_cached_uploader_seek,
  test_cached_uploader_complete
};

static GstS3Uploader*
test_cached_uploader_new (
  const GstS3UploaderConfig *config)
{
  TestCachedUploader *uploader = g_new(TestCachedUploader, 1);

  uploader->base.klass = &test_cached_uploader_class;
  uploader->upload_part_count = 0;
  uploader->upload_copy_part_count = 0;
  uploader->cache_hits = 0;
  uploader->cache_misses = 0;
  uploader->buffer_size = config->buffer_size;
  uploader->cache = new UploaderPartCache(config->cache_num_parts);

  return (GstS3Uploader*) uploader;
}
