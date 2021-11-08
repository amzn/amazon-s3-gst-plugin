/* amazon-s3-gst-plugin
 * Copyright (C) 2019 Marcin Kolny <marcin.kolny@gmail.com>
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
#ifndef __GST_AWS_CREDENTIALS_H__
#define __GST_AWS_CREDENTIALS_H__

#include <gst/gst.h>

G_BEGIN_DECLS

typedef struct _GstAWSCredentials GstAWSCredentials;

GST_EXPORT
GstAWSCredentials * gst_aws_credentials_new_default (void);

GST_EXPORT
GstAWSCredentials * gst_aws_credentials_copy (GstAWSCredentials * credentials);

GST_EXPORT
void gst_aws_credentials_free (GstAWSCredentials * credentials);

GST_EXPORT
GType gst_aws_credentials_get_type (void);

#define GST_TYPE_AWS_CREDENTIALS \
  (gst_aws_credentials_get_type())

G_END_DECLS

#endif /* __GST_AWS_CREDENTIALS_H__ */
