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
#include "gsts3downloader.h"

#include "gstawsutils.hpp"
#include "gstawsapihandle.hpp"
#include "gstawscredentials.hpp"
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
#include <aws/s3/S3Client.h>

#include <gst/gst.h>

struct _GstS3Downloader {
  _GstS3Downloader(const GstS3UploaderConfig *config);

  bool _init_downloader(const GstS3UploaderConfig *config);

  size_t download_part(char * buffer, size_t first, size_t last);

  Aws::String _bucket;
  Aws::String _key;

  std::shared_ptr<gst::aws::AwsApiHandle> _api_handle;
  std::unique_ptr<Aws::S3::S3Client> _s3_client;
};

_GstS3Downloader::_GstS3Downloader(const GstS3UploaderConfig *config) :
    _bucket(std::move(get_bucket_from_config(config))),
    _key(std::move(get_key_from_config(config))),
    _api_handle(config->init_aws_sdk ? gst::aws::AwsApiHandle::GetHandle() : nullptr)
{
}

bool _GstS3Downloader::_init_downloader(const GstS3UploaderConfig *config)
{
  Aws::Client::ClientConfiguration client_config;
  if (!is_null_or_empty(config->ca_file))
  {
    client_config.caFile = config->ca_file;
  }
  if (is_null_or_empty(config->region))
  {
    Aws::String region;
    if (!get_bucket_location(config->bucket, client_config, region))
    {
      // TODO report warning
    }
    else if (!region.empty())
    {
      client_config.region = std::move(region);
    }
  }
  else
  {
    client_config.region = config->region;
  }

  if (config->aws_sdk_request_timeout_ms != GST_S3_UPLOADER_CONFIG_DEFAULT_PROP_AWS_SDK_REQUEST_TIMEOUT) {
      client_config.requestTimeoutMs = config->aws_sdk_request_timeout_ms;
  }

  auto credentials_provider = gst_aws_credentials_create_provider(config->credentials);
  if (!credentials_provider)
  {
    return false;
  }

  // Configure AWS SDK specific client configuration
  if (!is_null_or_empty(config->aws_sdk_endpoint))
  {
    client_config.endpointOverride = Aws::String(config->aws_sdk_endpoint);
  }
  if (config->aws_sdk_use_http)
  {
    client_config.scheme = Aws::Http::Scheme::HTTP;
  }
  client_config.verifySSL = config->aws_sdk_verify_ssl;

  _s3_client = config->aws_sdk_s3_sign_payload ?
    std::unique_ptr<Aws::S3::S3Client>(new Aws::S3::S3Client(std::move(credentials_provider), client_config)) :
    std::unique_ptr<Aws::S3::S3Client>(new Aws::S3::S3Client(std::move(credentials_provider), client_config, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false));

  return true;
}

size_t
_GstS3Downloader::download_part(char* buffer, size_t first, size_t last)
{
  char *range = g_strdup_printf("bytes=%ld-%ld", first, last);

  Aws::S3::Model::GetObjectRequest request;
  request.WithBucket(_bucket)
    .WithKey(_key)
    .WithRange(range);

  g_free(range);


  auto outcome = _s3_client->GetObject(request);

  if (!outcome.IsSuccess()) {
    return 0;
  }

  return outcome.GetResult()
    .GetBody()
    .read(buffer, last-first)
    .gcount();
}

GstS3Downloader *
gst_s3_downloader_new (const GstS3UploaderConfig * config)
{
  g_return_val_if_fail (config, NULL);

  auto impl = new _GstS3Downloader(config);

  if (!impl->_init_downloader(config))
  {
    delete impl;
    return NULL;
  }

  return impl;
}

void
gst_s3_downloader_free (GstS3Downloader * downloader)
{
  delete reinterpret_cast<_GstS3Downloader *>(downloader);
}

gsize
gst_s3_downloader_download_part (GstS3Downloader * downloader,
    gchar * buffer, gsize first, gsize last)
{
  return reinterpret_cast<_GstS3Downloader *>(downloader)->download_part(buffer, first, last);
}
