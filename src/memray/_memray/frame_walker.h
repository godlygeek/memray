#pragma once

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <cstddef>
#include <string>

#include "debug_offsets.h"
#include "frame_layout.h"
#include "records.h"

namespace memray::frame_walker {

using FrameLayout = MemrayFrameLayout;

struct RawFrame
{
    PyCodeObject* code;  // valid only while the walk is in progress
    // Same units as PyFrame_GetLasti (bytes, or -1) from 3.11. Before 3.11,
    // the frame's raw `f_lasti`, which counts instructions in 3.10.
    int instruction_offset;
    bool is_entry_frame;  // first frame run by its _PyEval_EvalFrameDefault call
};

/**
 * Find the layout of the interpreter structures the walker reads.
 *
 * From 3.13, this comes from the debug offsets table (see loadDebugOffsets).
 * Before 3.13, it comes from CPython's internal headers at build time.
 *
 * Throws std::runtime_error if the layout can't be determined.
 * Doesn't require the GIL.
 */
FrameLayout
loadFrameLayout();

#if PY_VERSION_HEX >= 0x030D0000
using debug_offsets::DebugOffsets;

/**
 * Find the debug offsets table in `_PyRuntime`, validate it, and return a copy.
 *
 * Throws std::runtime_error if the table can't be found or fails validation.
 * Doesn't require the GIL.
 */
DebugOffsets
loadDebugOffsets();

/**
 * Check that a debug offsets table describes a runtime we know how to walk.
 *
 * Returns an empty string if it does, or a description of the problem.
 */
std::string
validateDebugOffsets(const DebugOffsets& table);

/**
 * Return the (unvalidated) debug offsets table in `_PyRuntime`, or a null
 * pointer if `_PyRuntime` can't be found.
 */
const DebugOffsets*
findRuntimeDebugOffsets();
#endif

/**
 * Make loadDebugOffsets use `table`, the raw bytes of a debug offsets table,
 * instead of the real table. Pass an empty string to go back to the real
 * table. For tests only.
 *
 * Throws std::invalid_argument if `table` has the wrong size, and
 * std::runtime_error for Python versions without a debug offsets table.
 */
void
setDebugOffsetsOverrideForTesting(const std::string& table);

/**
 * Walk the calling thread's Python frames, innermost first.
 *
 * Frames are filtered like PyEval_GetFrame and PyFrame_GetBack do: entry
 * frames owned by the C stack and incomplete frames are skipped.
 *
 * Returns the number of frames in the stack, and writes the first
 * `min(returned value, max)` of them to `out`. If the returned value is
 * larger than `max`, the caller can retry with a bigger buffer.
 *
 * Doesn't require the GIL, doesn't allocate, and doesn't touch refcounts.
 */
size_t
walkCurrentThread(const FrameLayout& layout, RawFrame* out, size_t max);

/**
 * Read the information the reader needs about a code object.
 *
 * Doesn't require the GIL. The caller must guarantee that `code` stays alive,
 * for instance because it's executing on the calling thread's stack.
 */
tracking_api::CodeObjectInfo
readCodeObjectInfo(PyCodeObject* code);

/**
 * Encode a `str` object as UTF-8 without the GIL.
 *
 * Unlike PyUnicode_AsUTF8, this never allocates a UTF-8 cache on the object
 * and never fails. Lone surrogates are replaced with U+FFFD.
 */
std::string
unicodeToUtf8(PyObject* str);

}  // namespace memray::frame_walker
