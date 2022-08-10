#pragma once

#include <sstream>
#include <gst/gst.h>
#include "gsts3uploader.h"
#include "gsts3uploaderconfig.h"

#include <gsts3uploaderpartcache.hpp>

using gst::aws::s3::UploaderPartCache;
using std::stringstream;

typedef struct {
    GstS3Uploader base;
    gint fail_upload_retry;
    gboolean fail_complete;

    UploaderPartCache *cache;

    gint permitted_cache_hits;

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
std::stringstream cached_buffer;

#define TEST_CACHED_UPLOADER(uploader) ((TestCachedUploader*) uploader)

static void
test_cached_uploader_reset_prev_stats()
{
  prev_test_cached_uploader_stats.upload_part_count = 0;
  prev_test_cached_uploader_stats.upload_copy_part_count = 0;
  prev_test_cached_uploader_stats.cache_hits = 0;
  prev_test_cached_uploader_stats.cache_misses = 0;
  cached_buffer.clear();
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
  gboolean ok = inst->fail_upload_retry != 0;

  inst->upload_part_count++;

  if (ok) {
    inst->fail_upload_retry--;

    // add this buffer to the cache.
    cached_buffer.write(buffer, size);
  }

  inst->cache->get_copy(inst->upload_part_count+1, next, next_size);
  if (*next != NULL)
    inst->cache_hits++;
  else if (*next_size > 0)
    inst->cache_misses++;
  inst->cache->insert_or_update(inst->upload_part_count, buffer, size);

  return ok;
}

static gboolean
test_cached_uploader_upload_part_copy (GstS3Uploader * uploader, G_GNUC_UNUSED const gchar * bucket,
  G_GNUC_UNUSED const gchar * key, G_GNUC_UNUSED gsize first, G_GNUC_UNUSED gsize last)
{
  gboolean ok = TEST_CACHED_UPLOADER(uploader)->fail_upload_retry != 0;

  TEST_CACHED_UPLOADER(uploader)->upload_copy_part_count++;

  if (ok) {
    TEST_CACHED_UPLOADER(uploader)->fail_upload_retry--;
  }

  return ok;
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
      cached_buffer.seekp (boffset, std::ios::beg);
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
  cached_buffer.seekp(0, std::ios::end);
  cached_buffer.flush();
  return !TEST_CACHED_UPLOADER(uploader)->fail_complete;
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
  const GstS3UploaderConfig *config,
  gint fail_upload_retry=-1,
  gboolean fail_complete=FALSE)
{
  TestCachedUploader *uploader = g_new(TestCachedUploader, 1);

  uploader->base.klass = &test_cached_uploader_class;
  uploader->fail_upload_retry = fail_upload_retry;
  uploader->fail_complete = fail_complete;
  uploader->upload_part_count = 0;
  uploader->upload_copy_part_count = 0;
  uploader->cache_hits = 0;
  uploader->cache_misses = 0;
  uploader->buffer_size = config->buffer_size;
  uploader->cache = new UploaderPartCache(config->cache_num_parts);

  return (GstS3Uploader*) uploader;
}
