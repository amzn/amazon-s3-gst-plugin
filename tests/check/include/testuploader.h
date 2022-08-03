#pragma once

#include <gst/gst.h>
#include "gsts3uploader.h"

typedef struct {
    GstS3Uploader base;
    gint fail_upload_retry;
    gboolean fail_complete;

    gint upload_part_count;
    gint upload_copy_part_count;
} TestUploader;

typedef struct {
  gint upload_part_count;
  gint upload_copy_part_count;
} TestUploaderStats;

static TestUploaderStats prev_test_uploader_stats = {0,};

#define TEST_UPLOADER(uploader) ((TestUploader*) uploader)

static void
test_uploader_reset_prev_stats()
{
  prev_test_uploader_stats.upload_part_count = 0;
  prev_test_uploader_stats.upload_copy_part_count = 0;
}

static void
test_uploader_destroy (GstS3Uploader * uploader)
{
  prev_test_uploader_stats.upload_part_count = TEST_UPLOADER(uploader)->upload_part_count;
  prev_test_uploader_stats.upload_copy_part_count = TEST_UPLOADER(uploader)->upload_copy_part_count;
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
test_uploader_upload_part_copy (GstS3Uploader * uploader, G_GNUC_UNUSED const gchar * bucket,
  G_GNUC_UNUSED const gchar * key, G_GNUC_UNUSED gsize first, G_GNUC_UNUSED gsize last)
{
  gboolean ok = TEST_UPLOADER(uploader)->fail_upload_retry != 0;

  TEST_UPLOADER(uploader)->upload_copy_part_count++;

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
  test_uploader_upload_part_copy,
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
  uploader->upload_copy_part_count = 0;

  return (GstS3Uploader*) uploader;
}
