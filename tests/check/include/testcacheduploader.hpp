#pragma once

#include <memory>
#include <gst/gst.h>
#include "gsts3uploader.h"

#include <gsts3uploaderpartcache.hpp>

using gst::aws::s3::UploaderPartCache;

typedef struct {
    GstS3Uploader base;
    gint fail_upload_retry;
    gboolean fail_complete;

    UploaderPartCache *cache;

    gint permitted_cache_hits;
    gboolean seek_performed;

    gint upload_part_count;
    gint upload_copy_part_count;
    gint cache_hits;
    gint cache_misses;
} TestCachedUploader;

typedef struct {
  gint upload_part_count;
  gint upload_copy_part_count;
  gint cache_hits;
  gint cache_misses;
} TestCachedUploaderStats;

static TestCachedUploaderStats prev_test_cached_uploader_stats;

#define TEST_CACHED_UPLOADER(uploader) ((TestCachedUploader*) uploader)

static void
test_cached_uploader_reset_prev_stats()
{
  prev_test_cached_uploader_stats.upload_part_count = 0;
  prev_test_cached_uploader_stats.upload_copy_part_count = 0;
  prev_test_cached_uploader_stats.cache_hits = 0;
  prev_test_cached_uploader_stats.cache_misses = 0;
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
test_cached_uploader_new (gint fail_upload_retry=-1, gboolean fail_complete=FALSE, gint cache_depth=0)
{
  TestCachedUploader *uploader = g_new(TestCachedUploader, 1);

  uploader->base.klass = &test_cached_uploader_class;
  uploader->fail_upload_retry = fail_upload_retry;
  uploader->fail_complete = fail_complete;
  uploader->upload_part_count = 0;
  uploader->upload_copy_part_count = 0;
  uploader->cache_hits = 0;
  uploader->cache_misses = 0;
  uploader->cache = new UploaderPartCache(cache_depth);

  return (GstS3Uploader*) uploader;
}
