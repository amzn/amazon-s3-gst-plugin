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
#ifndef __GST_S3_UPLOADER_H__
#define __GST_S3_UPLOADER_H__

#include <glib.h>

#include "gsts3uploaderconfig.h"

G_BEGIN_DECLS

typedef struct _GstS3Uploader GstS3Uploader;

typedef struct {
  void (*destroy) (GstS3Uploader *);
  gboolean (*upload_part) (GstS3Uploader *, const gchar *, gsize, gchar**, gsize*);
  gboolean (*upload_part_copy) (GstS3Uploader *, const gchar *, const gchar *, gsize, gsize);
  gboolean (*seek) (GstS3Uploader*, gsize, gchar**, gsize*);
  gboolean (*complete) (GstS3Uploader *);
} GstS3UploaderClass;

struct _GstS3Uploader {
  GstS3UploaderClass *klass;
};

GstS3Uploader *gst_s3_uploader_new_default (const GstS3UploaderConfig * config);

void gst_s3_uploader_destroy (GstS3Uploader * uploader);

/**
 * @brief Upload the buffer of some size as an optional part number.
 *
 * NOTE: if a 'seek' operation has been performed, caller is responsible
 * for handling a cache hit by managing the next buffer (even if not used)
 * or performing other operations on a cache miss if uploading more data
 * within the body of previously-uploaded data (i.e., complete, download,
 * begin uploading again).  A cache miss can be determined by the next
 * buffer being NULL with a non-zero next_size, which indicates the
 * uploader is aware of the part but does not have a buffer for it now.
 *
 * NOTE: If this is a re-upload of a part, the #size must match the
 * original size of the part unless this is the the tail of the upload in
 * progress.
 *
 * @param uploader The uploader instance
 * @param buffer The buffer to upload
 * @param size The size of the buffer
 * @param next The next buffer, if found in cache, or NULL
 * @param next_size The size of the #next buffer.
 * @return gboolean TRUE for success; FALSE otherwise.
 */
gboolean gst_s3_uploader_upload_part (GstS3Uploader * uploader,
  const gchar * buffer, gsize size, gchar **next, gsize *next_size);

/**
 * @brief Perform a Copy-Upload within S3 of some range as an optional
 * part number.  Use of this  API in concert with 'seek' is undefined.
 *
 * @param uploader The uploader instance
 * @param bucket The bucket where the copy is occurring
 * @param key The key for the bucket
 * @param first The start of the region to copy
 * @param last The end of the region to copy
 * @return gboolean TRUE for success; FALSE otherwise.
 */
gboolean gst_s3_uploader_upload_part_copy (GstS3Uploader * uploader,
  const gchar * bucket, const gchar * key, gsize first, gsize last);

/**
 * @brief Seek to an offset, if possible.
 *
 * NOTE: This feature must be enabled by configuration items.
 *
 * @param uploader The uploader instance
 * @param offset The offset into what the uploader has written.
 * @param buffer The buffer containing that offset or NULL if not found.
 * @param size The size of that buffer.
 * @return gboolean TRUE the buffer is valid; FALSE otherwise.
 */
gboolean gst_s3_uploader_seek (GstS3Uploader * uploader,
  gsize offset, gchar **buffer, gsize *size);

/**
 * @brief Finalize the S3 object.  This uploader will not be usable
 * afterwards.
 *
 * @param uploader The uploader instance
 * @return gboolean TRUE success; FALSE otherwise.
 */
gboolean gst_s3_uploader_complete (GstS3Uploader * uploader);

G_END_DECLS

#endif /* __GST_S3_UPLOADER_H__ */
