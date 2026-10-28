#pragma once

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Where to find the fields of interpreter structures that the frame walker
 * reads, as byte offsets. Only the fields that the running Python version has
 * are set.
 *
 * From 3.13, these come from the `_Py_DebugOffsets` table in `_PyRuntime`.
 * Earlier versions have no such table, so frame_layout_core.c fills them in
 * from CPython's internal headers instead. */
struct MemrayFrameLayout
{
    /* In the thread state: the current frame. In 3.11 and 3.12, the current
     * _PyCFrame instead, which holds the current frame. */
    uint64_t thread_state_current_frame;
    /* 3.11 and 3.12: the current frame in the _PyCFrame. */
    uint64_t cframe_current_frame;

    /* In the frame: a `PyFrameObject` before 3.11, or a
     * `_PyInterpreterFrame` from 3.11. */
    uint64_t frame_previous;
    uint64_t frame_executable;
    /* From 3.11: the instruction pointer. This is `prev_instr` in 3.11 and
     * 3.12, and `instr_ptr` from 3.13. */
    uint64_t frame_instr_ptr;
    /* Before 3.11: `f_lasti`. */
    uint64_t frame_lasti;
    /* From 3.11: the owner. */
    uint64_t frame_owner;
    /* 3.11 only: whether this is the first frame run by its call to
     * _PyEval_EvalFrameDefault. */
    uint64_t frame_is_entry;
    /* Free-threaded builds from 3.14: the index into the code object's
     * `co_tlbc` array of the thread-local copy of the bytecode that the
     * instruction pointer points into. */
    uint64_t frame_tlbc_index;
};

#if PY_VERSION_HEX < 0x030D0000
/* Fill in the layout for this Python version from its internal headers. */
void
memray_get_core_frame_layout(struct MemrayFrameLayout* layout);

/* Return the calling thread's thread state, or NULL if it has none or if it
 * isn't safe to read. Doesn't require the GIL. */
PyThreadState*
memray_get_walkable_thread_state(void);
#endif

#ifdef __cplusplus
}
#endif
