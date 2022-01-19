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

#include "gstawsutils.hpp"

#include <aws/s3/model/GetBucketLocationRequest.h>
#include <aws/s3/model/GetBucketLocationResult.h>
#include <aws/s3/S3Client.h>

bool get_bucket_location(const char* bucket_name, const Aws::Client::ClientConfiguration& client_config, Aws::String& location)
{
  Aws::S3::S3Client client(client_config, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);

  auto outcome = client.GetBucketLocation(Aws::S3::Model::GetBucketLocationRequest().WithBucket(bucket_name));
  if (!outcome.IsSuccess())
  {
    return false;
  }

  location = Aws::S3::Model::BucketLocationConstraintMapper::GetNameForBucketLocationConstraint(outcome.GetResult().GetLocationConstraint());
  return true;
}

bool is_null_or_empty(const char* str)
{
  return str == nullptr || strcmp(str, "") == 0;
}

const Aws::String get_bucket_from_config(const GstS3UploaderConfig * config)
{
    if (is_null_or_empty(config->location)) {
        return config->bucket;
    } else {
        GstUri *uri = gst_uri_from_string(config->location);
        Aws::String bucket(gst_uri_get_host(uri));
        gst_uri_unref(uri);
        return bucket;
    }
}

const Aws::String get_key_from_config(const GstS3UploaderConfig * config)
{
    if (is_null_or_empty(config->location)) {
        return config->key;
    } else {
        GstUri *uri = gst_uri_from_string(config->location);
        Aws::String path(gst_uri_get_path(uri));
        gst_uri_unref(uri);

        if (path[0] == '/') {
            return path.substr(1);
        } else {
            return path;
        }
    }
}