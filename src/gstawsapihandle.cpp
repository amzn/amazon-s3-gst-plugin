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

#include "gstawsapihandle.hpp"

#include <aws/core/Aws.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/utils/logging/LogSystemInterface.h>

namespace gst {
namespace aws {

class Logger : public ::Aws::Utils::Logging::LogSystemInterface
{
public:
    ::Aws::Utils::Logging::LogLevel GetLogLevel(void) const override
    {
        return _to_aws_log_level(gst_debug_category_get_threshold(gst_aws_s3_debug));
    }

    void Log(::Aws::Utils::Logging::LogLevel log_level, const char* tag, const char* format, ...) override
    {
        GstDebugLevel level = _to_gst_log_level(log_level);
        va_list varargs;
        va_start (varargs, format);

        if (G_UNLIKELY ((level) <= GST_LEVEL_MAX && (level) <= _gst_debug_min))
        {
            gst_debug_log_valist(gst_aws_s3_debug, level, "", tag, 0, NULL, format, varargs);
        }
        va_end (varargs);
    }

    void LogStream(::Aws::Utils::Logging::LogLevel log_level, const char* tag, const ::Aws::OStringStream &message_stream) override
    {
        Log(log_level, tag, "%s", message_stream.str().c_str());
    }

    void Flush() override
    {
    }

private:
    static ::Aws::Utils::Logging::LogLevel _to_aws_log_level(GstDebugLevel level)
    {
        using ::Aws::Utils::Logging::LogLevel;
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

    static GstDebugLevel _to_gst_log_level(::Aws::Utils::Logging::LogLevel level)
    {
        using ::Aws::Utils::Logging::LogLevel;
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
} // namespace aws
} // namespace s3

using gst::aws::AwsApiHandle;

AwsApiHandle::AwsApiHandle() {
    ::Aws::Utils::Logging::InitializeAWSLogging(::std::make_shared<Logger>());
    ::Aws::SDKOptions options;
    ::Aws::InitAPI(options);
}

AwsApiHandle::~AwsApiHandle()
{
    ::Aws::ShutdownAPI(::Aws::SDKOptions {});
    ::Aws::Utils::Logging::ShutdownAWSLogging();
}


::std::shared_ptr<AwsApiHandle> AwsApiHandle::GetHandle()
{
    static ::std::weak_ptr<AwsApiHandle> instance;
    if (auto ptr = instance.lock()) {
        return ptr;
    }

    ::std::shared_ptr<AwsApiHandle> ptr(new AwsApiHandle());
    instance = ptr;
    return ptr;
}
