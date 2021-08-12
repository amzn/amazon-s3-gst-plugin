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

#include "gsts3multipartuploader.h"

#include "gstawscredentials.hpp"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/logging/LogSystemInterface.h>
#include <aws/core/utils/ResourceManager.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/GetBucketLocationRequest.h>
#include <aws/s3/model/GetBucketLocationResult.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/S3Client.h>
#include <aws/sts/model/AssumeRoleRequest.h>
#include <aws/sts/STSClient.h>

#include <gst/gst.h>

GST_DEBUG_CATEGORY_EXTERN(gst_s3_sink_debug);

namespace gst
{
namespace aws
{
namespace s3
{
class Logger : public Aws::Utils::Logging::LogSystemInterface
{
public:
    Aws::Utils::Logging::LogLevel GetLogLevel(void) const override
    {
        return _to_aws_log_level(gst_debug_category_get_threshold(gst_s3_sink_debug));
    }

    void Log(Aws::Utils::Logging::LogLevel log_level, const char* tag, const char* format, ...) override
    {
        GstDebugLevel level = _to_gst_log_level(log_level);
        va_list varargs;
        va_start (varargs, format);

        if (G_UNLIKELY ((level) <= GST_LEVEL_MAX && (level) <= _gst_debug_min))
        {
            gst_debug_log_valist(gst_s3_sink_debug, level, "", tag, 0, NULL, format, varargs);
        }
        va_end (varargs);
    }

    void LogStream(Aws::Utils::Logging::LogLevel log_level, const char* tag, const Aws::OStringStream &message_stream) override
    {
        Log(log_level, tag, "%s", message_stream.str().c_str());
    }

    void Flush() override
    {
    }

private:
    static Aws::Utils::Logging::LogLevel _to_aws_log_level(GstDebugLevel level)
    {
        using Aws::Utils::Logging::LogLevel;
        switch (level)
        {
            case GST_LEVEL_NONE: return LogLevel::Off;
            case GST_LEVEL_ERROR: return LogLevel::Error;
            case GST_LEVEL_WARNING: return LogLevel::Warn;
            case GST_LEVEL_FIXME:
            case GST_LEVEL_INFO: return LogLevel::Info;
            case GST_LEVEL_DEBUG: return LogLevel::Debug;
            default: return LogLevel::Trace;
        }
    }

    static GstDebugLevel _to_gst_log_level(Aws::Utils::Logging::LogLevel level)
    {
        using Aws::Utils::Logging::LogLevel;
        switch (level)
        {
            case LogLevel::Off: return GST_LEVEL_NONE;
            case LogLevel::Fatal:
            case LogLevel::Error: return GST_LEVEL_ERROR;
            case LogLevel::Warn: return GST_LEVEL_WARNING;
            case LogLevel::Info: return GST_LEVEL_INFO;
            case LogLevel::Debug: return GST_LEVEL_DEBUG;
            default: return GST_LEVEL_TRACE;
        }
    }
};

class AwsApiHandle
{
    public:
        static std::shared_ptr<AwsApiHandle> GetHandle() {
            static std::weak_ptr<AwsApiHandle> instance;
            if (auto ptr = instance.lock()) {
                return ptr;
            }

            std::shared_ptr<AwsApiHandle> ptr(new AwsApiHandle());
            instance = ptr;
            return ptr;
        }

        virtual ~AwsApiHandle() {
            Aws::ShutdownAPI(Aws::SDKOptions {});
            Aws::Utils::Logging::ShutdownAWSLogging();
        }

    protected:
        AwsApiHandle() {
            Aws::Utils::Logging::InitializeAWSLogging(std::make_shared<Logger>());
            Aws::SDKOptions options;
            Aws::InitAPI(options);
        }

    private:
        AwsApiHandle(const AwsApiHandle&) = delete;
        AwsApiHandle& operator=(const AwsApiHandle&) = delete;
};

static bool get_bucket_location(const char* bucket_name, const Aws::Client::ClientConfiguration& client_config, Aws::String& location)
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

static bool is_null_or_empty(const char* str)
{
    return str == nullptr || strcmp(str, "") == 0;
}

using BufferManager = Aws::Utils::ExclusiveOwnershipResourceManager<uint8_t*>;

class PartState
{
public:
    explicit PartState(int part_number) :
        _part_number(part_number)
    {
    }

    int get_part_number() const
    {
        return _part_number;
    }

    Aws::String get_etag() const
    {
        return _etag;
    }
    void set_etag(Aws::String etag)
    {
        _etag = std::move(etag);
    }

    void set_md5_hash(Aws::Utils::ByteBuffer md5_hash)
    {
        _md5_hash = std::move(md5_hash);
    }

    bool verify_upload_outcome(const Aws::S3::Model::UploadPartOutcome& outcome) const
    {
        Aws::StringStream ss;
        ss << "\"" << Aws::Utils::HashingUtils::HexEncode(_md5_hash) << "\"";
        return ss.str() == outcome.GetResult().GetETag();
    }

private:
    Aws::Utils::ByteBuffer _md5_hash;
    Aws::String _etag;
    int _part_number;
};

using PartStateMap = std::map<int, PartState>;

class PartStateCollection
{
public:
    PartStateCollection(bool verify_hash) :
        _verify_hash(verify_hash)
    {
    }

    void start(PartState state)
    {
        std::lock_guard<std::mutex> l(_mtx);

        int num = state.get_part_number();
        _insert(_parts_in_flight, num, std::move(state));
    }

    void mark_part_as_completed(int part_number, const Aws::String& etag)
    {
        std::unique_lock<std::mutex> l(_mtx);

        PartState state = std::move(_parts_in_flight.at(part_number));
        _parts_in_flight.erase(part_number);
        state.set_etag(etag);
        _insert(_parts_completed, part_number, std::move(state));

        l.unlock();
        _upload_completed_cv.notify_one();
    }

    void mark_part_as_failed(int part_number)
    {
        std::unique_lock<std::mutex> l(_mtx);

        _insert(_parts_failed, part_number, std::move(_parts_in_flight.at(part_number)));
        _parts_in_flight.erase(part_number);

        l.unlock();
        _upload_completed_cv.notify_one();
    }

    size_t get_failed_parts_count() const
    {
        return _parts_failed.size();
    }

    bool verify_upload_outcome(int part_number, const Aws::S3::Model::UploadPartOutcome& outcome) const
    {
        if (!_verify_hash)
        {
            return true;
        }
        std::lock_guard<std::mutex> l(_mtx);
        return _parts_completed.at(part_number).verify_upload_outcome(outcome);
    }

    void wait_for_complete()
    {
        std::unique_lock<std::mutex> lk(_mtx);
        _upload_completed_cv.wait(lk, [this] { return _parts_in_flight.empty(); });
    }

    PartStateMap get_completed_parts() const
    {
        std::lock_guard<std::mutex> l(_mtx);
        return _parts_completed;
    }

    void clear()
    {
        std::lock_guard<std::mutex> l(_mtx);
        _parts_in_flight.clear();
        _parts_completed.clear();
        _parts_failed.clear();
    }

private:
    static void _insert(PartStateMap& map, int number, PartState part)
    {
        map.insert(std::make_pair(number, std::move(part)));
    }

    std::condition_variable _upload_completed_cv;
    mutable std::mutex _mtx;

    PartStateMap _parts_in_flight;
    PartStateMap _parts_completed;
    PartStateMap _parts_failed;

    bool _verify_hash;
};

class MultipartUploaderContext : public Aws::Client::AsyncCallerContext
{
public:
    MultipartUploaderContext(std::shared_ptr<PartStateCollection> states, std::shared_ptr<BufferManager> buffer_manager, int part_number) :
        _part_states(std::move(states)),
        _buffer_manager(std::move(buffer_manager)),
        _part_number(part_number)
    {
    }

    int get_part_number() const
    {
        return _part_number;
    }

    std::shared_ptr<BufferManager> get_buffer_manager() const
    {
        return _buffer_manager;
    }

    std::shared_ptr<PartStateCollection> get_part_states() const
    {
        return _part_states;
    }

private:
    std::shared_ptr<PartStateCollection> _part_states;
    std::shared_ptr<BufferManager> _buffer_manager;
    int _part_number;
};

class MultipartUploader
{
public:
    static std::unique_ptr<MultipartUploader> create(const GstS3UploaderConfig *config)
    {
        auto uploader = std::unique_ptr<MultipartUploader>(new MultipartUploader(config));
        if (!uploader->_init_uploader(config))
        {
            return nullptr;
        }
        return uploader;
    }

    ~MultipartUploader();

    bool upload(const char* data, size_t size);
    bool complete();

private:
    explicit MultipartUploader(const GstS3UploaderConfig *config);
    bool _init_uploader(const GstS3UploaderConfig * config);

    void _init_buffer_manager(size_t buffer_count, size_t buffer_size);

    std::unique_ptr<Aws::IOStream> _create_stream(const char* data, size_t size);

    static void _handle_upload_completed(const Aws::S3::S3Client*, const Aws::S3::Model::UploadPartRequest&, const Aws::S3::Model::UploadPartOutcome& outcome, const std::shared_ptr<const Aws::Client::AsyncCallerContext>& ctx);

    Aws::String _bucket;
    Aws::String _key;
    Aws::S3::Model::ObjectCannedACL _acl;

    Aws::S3::Model::CreateMultipartUploadOutcome _upload_outcome;

    std::shared_ptr<AwsApiHandle> _api_handle;
    std::unique_ptr<Aws::S3::S3Client> _s3_client;

    std::condition_variable _upload_completed_cv;
    std::shared_ptr<PartStateCollection> _part_states;

    std::shared_ptr<BufferManager> _buffer_manager;
    size_t _buffer_count = 0;

    int _part_counter = 0;
    bool _verify_hash = false;
};

// TODO: There's a few things I didn't implement because they're not critical (yet), but might
//       be needed in the (near) future:
//        * retry mechanism
//        * early failure notification - currently we notify user about the failure on complete() call
//        * implement hash verification - _verify_hash is already there, just need to expose it
//          as an option and test if works fine
//        * tests - not sure if AWS provide any infrastructure/framework for testing this kind of code,
//          or we have to rely on stable internet connection and run tests with credentials that allow
//          uploading/downloading files from S3.

MultipartUploader::MultipartUploader(const GstS3UploaderConfig *config) :
    _bucket(config->bucket),
    _key(config->key),
    _api_handle(config->init_aws_sdk ? AwsApiHandle::GetHandle() : nullptr),
    _part_states(std::make_shared<PartStateCollection>(false))
{
}

MultipartUploader::~MultipartUploader()
{
    if (_buffer_manager)
    {
        for (auto buffer : _buffer_manager->ShutdownAndWait(_buffer_count))
        {
            free(buffer);
        }
    }
}

void MultipartUploader::_init_buffer_manager(size_t buffer_count, size_t buffer_size)
{
    _buffer_manager = std::make_shared<BufferManager>();
    _buffer_count = buffer_count;

    for (size_t i = 0; i < buffer_count; i++)
    {
        _buffer_manager->PutResource(reinterpret_cast<uint8_t*>(malloc(buffer_size)));
    }
}

bool MultipartUploader::_init_uploader(const GstS3UploaderConfig * config)
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

    _init_buffer_manager(config->buffer_count, config->buffer_size);

    Aws::S3::Model::CreateMultipartUploadRequest upload_request;
    upload_request.SetBucket(_bucket);
    upload_request.SetKey(_key);

    if (!is_null_or_empty(config->acl))
    {
        _acl = Aws::S3::Model::ObjectCannedACLMapper::GetObjectCannedACLForName(Aws::String(config->acl));
        upload_request.SetACL(_acl);
    }

    if (is_null_or_empty(config->content_type))
    {
        upload_request.SetContentType("application/octet-stream");
    }
    else
    {
        upload_request.SetContentType(config->content_type);
    }

    _upload_outcome = _s3_client->CreateMultipartUpload(upload_request);
    return _upload_outcome.IsSuccess();
}

std::unique_ptr<Aws::IOStream> MultipartUploader::_create_stream(const char* data, size_t size)
{
    auto buffer = _buffer_manager->Acquire();
    memcpy(buffer, data, size);

    return std::unique_ptr<Aws::IOStream>(
        new Aws::IOStream(new Aws::Utils::Stream::PreallocatedStreamBuf(buffer, size)));
}

bool MultipartUploader::upload(const char* data, size_t size)
{
    int part_number = ++_part_counter;

    std::shared_ptr<Aws::IOStream> stream = _create_stream(data, size);
    Aws::S3::Model::UploadPartRequest request;
    request.WithBucket(_bucket)
        .WithKey(_key)
        .WithPartNumber(part_number)
        .WithUploadId(_upload_outcome.GetResult().GetUploadId())
        .WithContentLength(size);
    request.SetBody(stream);

    PartState part_state(part_number);

    if (_verify_hash)
    {
        Aws::Utils::ByteBuffer md5_of_stream(Aws::Utils::HashingUtils::CalculateMD5(*stream));
        request.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(md5_of_stream));
        part_state.set_md5_hash(md5_of_stream);
    }

    _part_states->start(std::move(part_state));

    auto context = std::make_shared<MultipartUploaderContext>(_part_states, _buffer_manager, part_number);

    _s3_client->UploadPartAsync(request, _handle_upload_completed, context);

    return true;
}

bool MultipartUploader::complete()
{
    _part_states->wait_for_complete();

    Aws::S3::Model::CompletedMultipartUpload completed_multipart_upload;
    for (const auto& part : _part_states->get_completed_parts())
    {
        Aws::S3::Model::CompletedPart completed_part;
        completed_part.SetETag(part.second.get_etag());
        completed_part.SetPartNumber(part.second.get_part_number());
        completed_multipart_upload.AddParts(completed_part);
    }

    size_t parts_failed_count = _part_states->get_failed_parts_count();
    _part_states->clear();

    Aws::S3::Model::CompleteMultipartUploadRequest upload_request;
    upload_request.SetBucket(_bucket);
    upload_request.SetKey(_key);
    upload_request.SetUploadId(_upload_outcome.GetResult().GetUploadId());

    upload_request.WithMultipartUpload(completed_multipart_upload);

    return parts_failed_count == 0 && _s3_client->CompleteMultipartUpload(upload_request).IsSuccess();
}

void MultipartUploader::_handle_upload_completed(const Aws::S3::S3Client*,
    const Aws::S3::Model::UploadPartRequest& request,
    const Aws::S3::Model::UploadPartOutcome& outcome,
    const std::shared_ptr<const Aws::Client::AsyncCallerContext>& ctx)
{
    auto context = std::static_pointer_cast<const MultipartUploaderContext>(ctx);

    auto original_stream_buffer = (Aws::Utils::Stream::PreallocatedStreamBuf*)request.GetBody()->rdbuf();
    context->get_buffer_manager()->Release(original_stream_buffer->GetBuffer());
    delete original_stream_buffer;

    auto states = context->get_part_states();
    int part_number = context->get_part_number();

    if (outcome.IsSuccess() && states->verify_upload_outcome(part_number, outcome))
    {
        states->mark_part_as_completed(part_number, outcome.GetResult().GetETag());
    }
    else
    {
        states->mark_part_as_failed(part_number);
    }
}

} // namespace s3
} // namespace aws
} // namespace gst

#define MULTIPART_UPLOADER_(uploader) reinterpret_cast<GstS3MultipartUploader*>(uploader)

using gst::aws::s3::MultipartUploader;

struct _GstS3MultipartUploader
{
  GstS3Uploader base;
  std::unique_ptr<MultipartUploader> impl;

  _GstS3MultipartUploader(std::unique_ptr<MultipartUploader> impl);
};

static void
gst_s3_multipart_uploader_destroy (GstS3Uploader * uploader)
{
  delete
  MULTIPART_UPLOADER_ (uploader);
}

static gboolean
gst_s3_multipart_uploader_upload_part (GstS3Uploader *
    uploader, const gchar * buffer, gsize size)
{
  GstS3MultipartUploader *self = MULTIPART_UPLOADER_ (uploader);
  g_return_val_if_fail (self && self->impl, FALSE);
  return self->impl->upload (buffer, size);
}

static gboolean
gst_s3_multipart_uploader_complete (GstS3Uploader * uploader)
{
  GstS3MultipartUploader *self = MULTIPART_UPLOADER_ (uploader);
  g_return_val_if_fail (self && self->impl, FALSE);
  return self->impl->complete ();
}
static GstS3UploaderClass default_class = {
  gst_s3_multipart_uploader_destroy,
  gst_s3_multipart_uploader_upload_part,
  gst_s3_multipart_uploader_complete
};

GstS3Uploader *
gst_s3_multipart_uploader_new (const GstS3UploaderConfig * config)
{
  g_return_val_if_fail (config, NULL);

  auto impl = MultipartUploader::create(config);

  if (!impl)
  {
    return NULL;
  }

  return reinterpret_cast < GstS3Uploader * >(new GstS3MultipartUploader (std::move (impl)));
}

_GstS3MultipartUploader::_GstS3MultipartUploader(std::unique_ptr<MultipartUploader> impl) :
    impl(std::move(impl))
{
  base.klass = &default_class;
}
