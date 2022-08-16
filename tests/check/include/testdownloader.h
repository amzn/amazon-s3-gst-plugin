#pragma once

#include <gst/gst.h>
#include "gsts3downloader.h"

typedef struct {
  GstS3Downloader base;

  size_t bytes_downloaded;
  gint downloads_requested;
} TestDownloader;

#define TEST_DOWNLOADER(downloader) ((TestDownloader*) downloader)

// Simply returns that the amount requested was actually returned.
static size_t
test_downloader_download_part (
  G_GNUC_UNUSED GstS3Downloader *downloader,
  G_GNUC_UNUSED char *buffer,
  size_t first,
  size_t last)
{
  size_t requested = last - first;
  TEST_DOWNLOADER(downloader)->bytes_downloaded += requested;
  TEST_DOWNLOADER(downloader)->downloads_requested++;
  return requested;
}

static void
test_downloader_destroy (GstS3Downloader *downloader)
{
  g_free(downloader);
}

static GstS3DownloaderClass test_downloader_class = {
  test_downloader_destroy,
  test_downloader_download_part
};

static GstS3Downloader *
test_downloader_new ()
{
  TestDownloader *downloader = g_new (TestDownloader, 1);
  downloader->base.klass = &test_downloader_class;
  downloader->bytes_downloaded = 0;
  downloader->downloads_requested = 0;
  return (GstS3Downloader*) downloader;
}
