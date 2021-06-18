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

#ifndef __GST_AWS_UTILS_HPP__
#define __GST_AWS_UTILS_HPP__

#include <aws/core/client/ClientConfiguration.h>

bool get_bucket_location   (const char* bucket_name,
                            const Aws::Client::ClientConfiguration& client_config,
                            Aws::String& location);

bool is_null_or_empty  (const char* str);

#endif // __GST_AWS_UTILS_HPP__
