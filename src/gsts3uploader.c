#include "gsts3uploader.h"

#define GET_CLASS_(uploader) ((GstS3Uploader*) (uploader))->klass

void
gst_s3_uploader_destroy (GstS3Uploader * uploader)
{
  GET_CLASS_ (uploader)->destroy (uploader);
}

gboolean
gst_s3_uploader_upload_part (GstS3Uploader * uploader,
    const gchar * buffer, gsize size)
{
  return GET_CLASS_ (uploader)->upload_part (uploader, buffer, size);
}

gboolean
gst_s3_uploader_complete (GstS3Uploader * uploader)
{
  return GET_CLASS_ (uploader)->complete (uploader);
}
