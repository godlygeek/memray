/* Layouts of interpreter structures for Python versions before 3.13.
 *
 * Python 3.13 and later describe the layout of the structures the frame
 * walker reads in their `_Py_DebugOffsets` table. Earlier versions don't have
 * that table, so this file gets the same information from CPython's internal
 * headers. Those require Py_BUILD_CORE, and aren't valid C++, so this is kept
 * to a single C file that exposes only plain offsets and a way to find the
 * calling thread's thread state. */

/* patchlevel.h only defines version macros, so it can be included before we
 * decide whether to define Py_BUILD_CORE. */
#include <patchlevel.h>

#if PY_VERSION_HEX < 0x030D0000

#    define PY_SSIZE_T_CLEAN
#    define Py_BUILD_CORE 1
#    include <Python.h>

#    include "frameobject.h"
#    include "internal/pycore_runtime.h"
#    if PY_VERSION_HEX >= 0x030B0000
#        include "internal/pycore_frame.h"
#    endif

#    include <stddef.h>
#    include <string.h>

#    include "frame_layout.h"

void
memray_get_core_frame_layout(struct MemrayFrameLayout* layout)
{
    memset(layout, 0, sizeof(*layout));
#    if PY_VERSION_HEX < 0x030B0000
    layout->thread_state_current_frame = offsetof(PyThreadState, frame);
    layout->frame_previous = offsetof(PyFrameObject, f_back);
    layout->frame_executable = offsetof(PyFrameObject, f_code);
    layout->frame_lasti = offsetof(PyFrameObject, f_lasti);
#    else
    layout->thread_state_current_frame = offsetof(PyThreadState, cframe);
    layout->cframe_current_frame = offsetof(_PyCFrame, current_frame);
    layout->frame_previous = offsetof(_PyInterpreterFrame, previous);
    layout->frame_executable = offsetof(_PyInterpreterFrame, f_code);
    layout->frame_instr_ptr = offsetof(_PyInterpreterFrame, prev_instr);
    layout->frame_owner = offsetof(_PyInterpreterFrame, owner);
#        if PY_VERSION_HEX < 0x030C0000
    layout->frame_is_entry = offsetof(_PyInterpreterFrame, is_entry);
#        endif
#    endif
}

PyThreadState*
memray_get_walkable_thread_state(void)
{
    PyThreadState* tstate = NULL;

#    if PY_VERSION_HEX >= 0x030C0000
    /* In 3.12 this is a thread-local: the calling thread's attached thread
     * state, or NULL if it released the GIL. Before 3.12 it's a global
     * holding the thread state of whichever thread holds the GIL, which may
     * not be the calling thread, so it can't be used. */
    tstate = _PyThreadState_UncheckedGet();
    if (tstate) {
        return tstate;
    }
#    endif

    /* The thread state bound to this thread for the PyGILState API, which
     * stays set when the thread releases the GIL. */
    tstate = PyGILState_GetThisThreadState();
    if (!tstate) {
        return NULL;
    }

    /* During finalization, the thread states of threads other than the one
     * finalizing the interpreter may be freed, so don't follow them. */
    PyThreadState* finalizing = _PyRuntimeState_GetFinalizing(&_PyRuntime);
    if (finalizing && finalizing != tstate) {
        return NULL;
    }
    return tstate;
}

#else

/* Nothing is needed from 3.13, but ISO C forbids an empty file. */
typedef int memray_frame_layout_core_unused;

#endif
