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
#ifndef __GST_AWS_CREDENTIALS_HPP__
#define __GST_AWS_CREDENTIALS_HPP__

#include "gstawscredentials.h"

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <functional>

using GstAWSCredentialsProviderFactory = std::function<std::unique_ptr<Aws::Auth::AWSCredentialsProvider>()>;

GST_EXPORT
GstAWSCredentials *
gst_aws_credentials_new (GstAWSCredentialsProviderFactory factory);

GST_EXPORT
std::unique_ptr<Aws::Auth::AWSCredentialsProvider>
gst_aws_credentials_create_provider (GstAWSCredentials * credentials);

#endif /* __GST_AWS_CREDENTIALS_HPP__ */
