#pragma once

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <cstdint>

namespace memray::debug_offsets {

// Copies of the layout of CPython's `_Py_DebugOffsets` table, up to and
// including `code_object`, for each supported minor version. The 3.13 table
// is in `Include/internal/pycore_runtime.h`; later ones are in
// `Include/internal/pycore_debug_offsets.h`.
//
// CPython stores this table as the first member of `_PyRuntime` so that
// out-of-process tools can find the layout of internal structures without
// including internal headers. We read it in-process for the same reason.
//
// These must match the field order of `_Py_DebugOffsets` exactly. The table's
// layout is only guaranteed to be stable between patch releases of a single
// minor version, so the version is validated before the table is used.

struct DebugOffsets313
{
    char cookie[8];
    uint64_t version;
    uint64_t free_threaded;

    struct
    {
        uint64_t size;
        uint64_t finalizing;
        uint64_t interpreters_head;
    } runtime_state;

    struct
    {
        uint64_t size;
        uint64_t id;
        uint64_t next;
        uint64_t threads_head;
        uint64_t gc;
        uint64_t imports_modules;
        uint64_t sysdict;
        uint64_t builtins;
        uint64_t ceval_gil;
        uint64_t gil_runtime_state;
        uint64_t gil_runtime_state_enabled;
        uint64_t gil_runtime_state_locked;
        uint64_t gil_runtime_state_holder;
    } interpreter_state;

    struct
    {
        uint64_t size;
        uint64_t prev;
        uint64_t next;
        uint64_t interp;
        uint64_t current_frame;
        uint64_t thread_id;
        uint64_t native_thread_id;
        uint64_t datastack_chunk;
        uint64_t status;
    } thread_state;

    struct
    {
        uint64_t size;
        uint64_t previous;
        uint64_t executable;
        uint64_t instr_ptr;
        uint64_t localsplus;
        uint64_t owner;
    } interpreter_frame;

    struct
    {
        uint64_t size;
        uint64_t filename;
        uint64_t name;
        uint64_t qualname;
        uint64_t linetable;
        uint64_t firstlineno;
        uint64_t argcount;
        uint64_t localsplusnames;
        uint64_t localspluskinds;
        uint64_t co_code_adaptive;
    } code_object;
};

struct DebugOffsets314
{
    char cookie[8];
    uint64_t version;
    uint64_t free_threaded;

    struct
    {
        uint64_t size;
        uint64_t finalizing;
        uint64_t interpreters_head;
    } runtime_state;

    struct
    {
        uint64_t size;
        uint64_t id;
        uint64_t next;
        uint64_t threads_head;
        uint64_t threads_main;
        uint64_t gc;
        uint64_t imports_modules;
        uint64_t sysdict;
        uint64_t builtins;
        uint64_t ceval_gil;
        uint64_t gil_runtime_state;
        uint64_t gil_runtime_state_enabled;
        uint64_t gil_runtime_state_locked;
        uint64_t gil_runtime_state_holder;
        uint64_t code_object_generation;
        uint64_t tlbc_generation;
    } interpreter_state;

    struct
    {
        uint64_t size;
        uint64_t prev;
        uint64_t next;
        uint64_t interp;
        uint64_t current_frame;
        uint64_t thread_id;
        uint64_t native_thread_id;
        uint64_t datastack_chunk;
        uint64_t status;
    } thread_state;

    struct
    {
        uint64_t size;
        uint64_t previous;
        uint64_t executable;
        uint64_t instr_ptr;
        uint64_t localsplus;
        uint64_t owner;
        uint64_t stackpointer;
        uint64_t tlbc_index;
    } interpreter_frame;

    struct
    {
        uint64_t size;
        uint64_t filename;
        uint64_t name;
        uint64_t qualname;
        uint64_t linetable;
        uint64_t firstlineno;
        uint64_t argcount;
        uint64_t localsplusnames;
        uint64_t localspluskinds;
        uint64_t co_code_adaptive;
        uint64_t co_tlbc;
    } code_object;
};

struct DebugOffsets315
{
    char cookie[8];
    uint64_t version;
    uint64_t free_threaded;

    struct
    {
        uint64_t size;
        uint64_t finalizing;
        uint64_t interpreters_head;
    } runtime_state;

    struct
    {
        uint64_t size;
        uint64_t id;
        uint64_t next;
        uint64_t threads_head;
        uint64_t threads_main;
        uint64_t gc;
        uint64_t imports_modules;
        uint64_t sysdict;
        uint64_t builtins;
        uint64_t ceval_gil;
        uint64_t gil_runtime_state;
        uint64_t gil_runtime_state_enabled;
        uint64_t gil_runtime_state_locked;
        uint64_t gil_runtime_state_holder;
        uint64_t code_object_generation;
        uint64_t tlbc_generation;
    } interpreter_state;

    struct
    {
        uint64_t size;
        uint64_t prev;
        uint64_t next;
        uint64_t interp;
        uint64_t current_frame;
        uint64_t base_frame;
        uint64_t last_profiled_frame;
        uint64_t last_profiled_frame_seq;
        uint64_t thread_id;
        uint64_t native_thread_id;
        uint64_t datastack_chunk;
        uint64_t status;
        uint64_t holds_gil;
        uint64_t gil_requested;
        uint64_t current_exception;
        uint64_t exc_state;
    } thread_state;

    struct
    {
        uint64_t exc_value;
    } err_stackitem;

    struct
    {
        uint64_t size;
        uint64_t previous;
        uint64_t executable;
        uint64_t instr_ptr;
        uint64_t localsplus;
        uint64_t owner;
        uint64_t stackpointer;
        uint64_t tlbc_index;
    } interpreter_frame;

    struct
    {
        uint64_t size;
        uint64_t filename;
        uint64_t name;
        uint64_t qualname;
        uint64_t linetable;
        uint64_t firstlineno;
        uint64_t argcount;
        uint64_t localsplusnames;
        uint64_t localspluskinds;
        uint64_t co_code_adaptive;
        uint64_t co_tlbc;
    } code_object;
};

// Values of `_PyInterpreterFrame.owner` (`enum _frameowner` in
// `Include/internal/pycore_frame.h` from 3.11, and in
// `Include/internal/pycore_interpframe_structs.h` from 3.14), which exists
// from 3.11. These aren't in the offsets table, and the internal headers
// can't be included from C++, so they're copied here for each version.
enum FrameOwner : char {
    FRAME_OWNED_BY_THREAD = 0,
    FRAME_OWNED_BY_GENERATOR = 1,
    FRAME_OWNED_BY_FRAME_OBJECT = 2,
#if PY_VERSION_HEX >= 0x030E0000
    FRAME_OWNED_BY_INTERPRETER = 3,
#endif
#if PY_VERSION_HEX >= 0x030C0000 && PY_VERSION_HEX < 0x030E0000
    FRAME_OWNED_BY_CSTACK = 3,
#elif PY_VERSION_HEX >= 0x030E0000 && PY_VERSION_HEX < 0x030F0000
    FRAME_OWNED_BY_CSTACK = 4,
#endif
};

#if PY_VERSION_HEX >= 0x030D0000 && PY_VERSION_HEX < 0x030E0000
using DebugOffsets = DebugOffsets313;
#elif PY_VERSION_HEX >= 0x030E0000 && PY_VERSION_HEX < 0x030F0000
using DebugOffsets = DebugOffsets314;
#elif PY_VERSION_HEX >= 0x030F0000 && PY_VERSION_HEX < 0x03100000
using DebugOffsets = DebugOffsets315;
#endif

// Frames that `PyEval_GetFrame` and `PyFrame_GetBack` never return: entry
// frames owned by the C stack, and frames owned by the interpreter itself.
// From 3.13 `_PyFrame_IsIncomplete` checks for these owners; in 3.12 their
// trampoline code object is never complete. In every version from 3.11,
// these are the owners with values of 3 or more.
inline bool
isNeverCompleteOwner(char owner)
{
    return owner >= 3;
}

#if PY_VERSION_HEX >= 0x030C0000
// The owner of a frame's `previous` frame when `_PyFrame_IsEntryFrame` would
// say that the frame is an entry frame, meaning that it's the first frame run
// by a call to `_PyEval_EvalFrameDefault`. From 3.12 only: 3.11 has an
// `is_entry` flag on the frame itself instead.
inline bool
isEntryFramePreviousOwner(char owner)
{
#    if PY_VERSION_HEX < 0x030E0000
    return owner == FRAME_OWNED_BY_CSTACK;
#    else
    // From 3.14, the entry frames that _PyEval_EvalFrameDefault pushes are
    // owned by the interpreter.
    return owner == FRAME_OWNED_BY_INTERPRETER;
#    endif
}
#endif

}  // namespace memray::debug_offsets
