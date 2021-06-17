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
#ifndef __AWS_API__
#define __AWS_API__

#include <memory>

namespace gst {
namespace aws {

class AwsApiHandle
{
    public:
        static std::shared_ptr<AwsApiHandle> GetHandle();
        virtual ~AwsApiHandle();

    protected:
        AwsApiHandle();

    private:
        AwsApiHandle(const AwsApiHandle&) = delete;
        AwsApiHandle& operator=(const AwsApiHandle&) = delete;
};

} // namespace aws
} // namespace gst

#endif /* __AWS_API__ */
