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

#pragma once

#include <gst/gst.h>
#include <vector>
#include <string>

GST_DEBUG_CATEGORY_EXTERN(gst_s3_sink_debug);

namespace gst::aws::s3 {

class UploaderPartCache {
public:
    /**
     * @brief Construct a new UploaderPartCache with depth.  A negative depth
     * means keep track of the last N-depth parts' buffers in the cache.
     * A positive depth means to track the first 'depth' buffers in the
     * cache.  0 means disabled, cache no buffers (but will track size).
     *
     * @param depth The number of parts to cache buffers.
     */
    UploaderPartCache (int depth=0) : 
        _cache_num_parts(depth)
    { /* empty */ }

    inline int size () { return _cache.size(); }

    bool
    insert_or_update (int part_num, const char *buffer, size_t _size)
    {
        int part_idx = part_num - 1;
        if (part_idx > size())
            return false; // out of range

        if (part_idx == size())
        {
            // Inserting a new one.
            // NOTE: will deal with buffer later if necessary
            _cache.push_back(PartInfo(NULL, _size));
        }
        else {
            // update size, at a minimum
            _cache[part_num-1].size(_size);
        }

        // Update the buffers in the cache
        if (_cache_num_parts != 0) {
            int first = 0;
            int last = 0;

            if (0 < _cache_num_parts) {
                // Keeping up to the first N buffers
                // We only need to do this if we
                // Positive idx = [0, _cache_num_parts-1]
                last = _cache_num_parts - 1;
                first = 0;
            }
            else if (_cache_num_parts < 0) {
                // _cache_num_parts is negative
                // Keeping the last N buffers.
                last = size();
                first = MAX(last + _cache_num_parts, 0);
            }

            for (int i = 0; i < size(); i++) {
                if (i < first || last < i) {
                    _cache[i].clear_buffer();
                }
                else if (i == part_num-1) {
                    // The item we just added or updated will need its buffer
                    // in the cache.  If the size doesn't match, update the
                    // buffer so it does and then copy over the incoming data.
                    if (_cache[i].size() != _size)
                        _cache[i].clear_buffer();
                    _cache[i].size(_size);

                    if (buffer) {
                        // Incoming buffer present; copy it.
                        _cache[i].buffer(buffer, _size);
                    }
                    else {
                        // Previously this had a buffer but this 'update' had a NULL
                        // buffer passed in, so this buffer should get cleared as
                        // that is "The Update".
                        _cache[i].clear_buffer();
                    }
                }
            }
        }
        return true;
    } 

    bool
    get_copy (int part_num, char **buffer, size_t *_size)
    {
        if (size() == 0 || 0 >= part_num || part_num > size())
            return false;

        *buffer = _cache[part_num - 1].copy_buffer();
        *_size = _cache[part_num - 1].size();
        return true;
    }

    bool
    find (size_t offset, int *part_num, char **buffer, size_t *_size)
    {
        int i = 1;
        size_t start = 0;

        for (auto &item : _cache) {
            size_t end = start + item.size();

            if (start <= offset && offset < end) {
                get_copy(i, buffer, _size);
                *part_num = i;
                return true;
            }
            i++;
            start += item.size();
        }
        return false;
    }
    struct PartInfo {
        public:
        // Constructor
        PartInfo(const char* in_buffer=NULL, size_t in_size=0) :
            _buffer(),
            _size(in_size)
        {
            buffer(in_buffer, in_size);
        }

        // move constructor
        PartInfo (PartInfo &&other) {
            _buffer = other._buffer;
            _size = other._size;

            other._buffer = NULL;
            other._size = 0;
        }

        ~PartInfo() {
            clear_buffer();
        }

        // Copy
        PartInfo (const PartInfo &rhs) {
            _buffer = rhs.copy_buffer();
            _size = rhs.size();
        }

        // Assign
        PartInfo& operator=(const PartInfo &rhs) {
            _buffer = rhs.copy_buffer();
            _size = rhs.size();
            return *this;
        }

        // Move
        PartInfo& operator=(PartInfo&& other) {
            if (this != &other) {
                clear_buffer();

                _buffer = other._buffer;
                _size = other._size;

                other._buffer = NULL;
                other._size = 0;
            }
            return *this;
        }

        void clear_buffer() {
            if (_buffer) {
                free(_buffer);
            }
            _buffer = NULL;
        }

        void buffer(const char *in, size_t in_size) {
            if (in_size != size()) {
                clear_buffer();
            }
            size(in_size);

            if (in) {
                if (!_buffer && !(_buffer = new char[in_size])) {
                    GST_ERROR("Failed to allocate a cache buffer of size %ld", in_size);
                    return;
                }
                memcpy(_buffer, in, sizeof(char) * size());
            }
            else if (_buffer) {
                clear_buffer();
            }
        }

        const char* buffer () const { return _buffer; }

        char* copy_buffer () const {
            char* out = NULL;
            if (_buffer && size()) {
                if ((out = new char[_size])) {
                    memcpy(out, _buffer, sizeof(char) * size());
                }
                else {
                    GST_ERROR("Failed to allocate a cache buffer of size %ld", size());
                }
            }
            return out;
        }

        void size(size_t s) { _size = s; }

        size_t size() const { return _size; }

        private:
        char* _buffer;
        size_t _size;
    };

private:
    std::vector<PartInfo> _cache;
    int _cache_num_parts;
};

}; // gst::aws::s3