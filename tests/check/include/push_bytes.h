#pragma once

#include <gst/gst.h>

GstBuffer*
val_filled_buffer_new (size_t num_bytes, guint8 val)
{
  GstBuffer *buf = gst_buffer_new_and_alloc(num_bytes);
  GstMapInfo info;
  guint i;

  if (!gst_buffer_map (buf, &info, GST_MAP_WRITE)) {
    goto buffer_map_failed;
  }

  for (i = 0; i < num_bytes; ++i)
    ((guint8 *)info.data)[i] = val & 0xff;
  gst_buffer_unmap (buf, &info);

out:
  return buf;

buffer_map_failed:
  gst_buffer_unref (buf);
  buf = NULL;
  goto out;
}

GstBuffer*
random_buffer_new (size_t num_bytes)
{
  GstBuffer *buf = gst_buffer_new_and_alloc(num_bytes);
  GRand *rand = g_rand_new_with_seed (num_bytes);
  GstMapInfo info;
  guint i;

  if (!gst_buffer_map (buf, &info, GST_MAP_WRITE)) {
    goto buffer_map_failed;
  }

  for (i = 0; i < num_bytes; ++i)
    ((guint8 *)info.data)[i] = (g_rand_int (rand) >> 24) & 0xff;
  gst_buffer_unmap (buf, &info);

out:
  g_rand_free (rand);
  return buf;

buffer_map_failed:
  gst_buffer_unref (buf);
  buf = NULL;
  goto out;
}

static gboolean
push_buffer (GstPad *pad, GstBuffer* buf, GstFlowReturn expected_ret_code)
{
  gboolean ret = FALSE;

  if (buf) {
    ret = gst_pad_push (pad, buf) == expected_ret_code;
  }

  return ret;
}

#define PUSH_BYTES(pad, num_bytes) fail_if (!push_buffer (pad, random_buffer_new (num_bytes), GST_FLOW_OK))
#define PUSH_BYTES_FAILURE(pad, num_bytes) fail_if (!push_buffer (pad, random_buffer_new (num_bytes), GST_FLOW_ERROR))

#define PUSH_VAL_BYTES(pad, num_bytes, val) fail_if (!push_buffer (pad, val_filled_buffer_new (num_bytes, val), GST_FLOW_OK))
#define PUSH_VAL_BYTES_FAILURE(pad, num_bytes, val) fail_if (!push_buffer (pad, val_filled_buffer_new (num_bytes, val), GST_FLOW_ERROR))

/**
 * @brief Before pushing buffers/bytes one must send a stream start and a segment declaration
 * or else critical errors will occur.
 *
 * @param srcpad The pad to send the events onto.
 * @param stream_name nullable, the name of the stream
 * @return gboolean TRUE if the events were set; FALSE otherwise.
 */
static gboolean
prepare_to_push_bytes (GstPad *srcpad, const char* stream_name) {
  gboolean ret = TRUE;
  GstSegment segment;
  const char* name = (stream_name) ? stream_name : "test";

  gst_segment_init(&segment, GST_FORMAT_BYTES);
  ret = (
    gst_pad_push_event(srcpad, gst_event_new_stream_start(name))
    &&
    gst_pad_push_event(srcpad, gst_event_new_segment(&segment))
  );

  return ret;
}
