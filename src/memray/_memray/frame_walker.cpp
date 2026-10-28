#include "frame_walker.h"

#include <cassert>
#include <cstring>
#include <dlfcn.h>
#include <optional>
#include <stdexcept>

#include "compat.h"

#if PY_VERSION_HEX < 0x03090000 || PY_VERSION_HEX >= 0x03100000
#    error "The frame walker only supports CPython 3.9 through 3.15"
#endif
namespace memray::frame_walker {

using namespace debug_offsets;

namespace {

#if PY_VERSION_HEX >= 0x030D0000
std::optional<DebugOffsets> s_override;
#endif

// `_Py_CODEUNIT` is internal from 3.13. Each code unit is 2 bytes, holding an
// opcode and its argument (or a cache entry).
using CodeUnit = uint16_t;

template<typename T>
inline T
field(const void* ptr, uint64_t offset)
{
    T ret;
    memcpy(&ret, static_cast<const char*>(ptr) + offset, sizeof(T));
    return ret;
}

void
appendUtf8(std::string& out, Py_UCS4 ch)
{
    if (ch >= 0xD800 && ch <= 0xDFFF) {
        ch = 0xFFFD;  // A lone surrogate; it can't be encoded.
    }

    if (ch < 0x80) {
        out.push_back(static_cast<char>(ch));
    } else if (ch < 0x800) {
        out.push_back(static_cast<char>(0xC0 | (ch >> 6)));
        out.push_back(static_cast<char>(0x80 | (ch & 0x3F)));
    } else if (ch < 0x10000) {
        out.push_back(static_cast<char>(0xE0 | (ch >> 12)));
        out.push_back(static_cast<char>(0x80 | ((ch >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (ch & 0x3F)));
    } else {
        out.push_back(static_cast<char>(0xF0 | (ch >> 18)));
        out.push_back(static_cast<char>(0x80 | ((ch >> 12) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | ((ch >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (ch & 0x3F)));
    }
}

// Read the frame's code object (`f_executable`, or `f_code` before 3.13),
// returning a null pointer if it isn't an object.
inline PyObject*
readExecutable(const void* frame, uint64_t offset)
{
#if PY_VERSION_HEX < 0x030E0000
    return field<PyObject*>(frame, offset);
#else
    // From 3.14 this is a `_PyStackRef`, a tagged pointer. In builds with a GIL,
    // the low bit is set for deferred references and both low bits are set
    // for tagged integers, which aren't objects. Objects are aligned to more
    // than 4 bytes, so the low two bits of a real pointer are always clear.
    auto bits = field<uintptr_t>(frame, offset);
    const uintptr_t TAG_BITS = 3;
    if ((bits & TAG_BITS) == TAG_BITS) {
        return nullptr;
    }
    return reinterpret_cast<PyObject*>(bits & ~TAG_BITS);
#endif
}

#if PY_VERSION_HEX >= 0x030B0000
inline bool
pointsInto(const CodeUnit* ptr, const CodeUnit* start, Py_ssize_t num_code_units)
{
    auto addr = reinterpret_cast<uintptr_t>(ptr);
    auto begin = reinterpret_cast<uintptr_t>(start);
    return addr >= begin && addr <= begin + num_code_units * sizeof(CodeUnit);
}

// Return the start of the copy of `code`'s bytecode that the frame's
// instruction pointer `instr` points into, or a null pointer if it can't be
// determined.
inline const CodeUnit*
frameBytecode(
        [[maybe_unused]] const FrameLayout& layout,
        [[maybe_unused]] const void* frame,
        PyCodeObject* code,
        [[maybe_unused]] const CodeUnit* instr)
{
    auto main_copy = reinterpret_cast<const CodeUnit*>(code->co_code_adaptive);
#    if defined(Py_GIL_DISABLED) && PY_VERSION_HEX >= 0x030E0000
    // From 3.14, each thread in a free-threaded build runs its own copy of
    // the bytecode, found at the frame's index into the code object's
    // `co_tlbc` array. Index 0 is the main copy, stored in the code object.
    //
    // Copies are only freed by _Py_ClearUnusedTLBC, for indexes that no
    // live thread holds. We never dereference a copy anyway, only compare
    // the instruction pointer against it, so a freed copy can't hurt us.
    //
    // The array itself is another matter. When another thread needs an index
    // past its end, create_tlbc_lock_held publishes a bigger array and passes
    // the old one to _PyMem_FreeDelayed. That frees it once QSBR's read
    // sequence passes the goal, and qsbr_poll_scan skips detached threads
    // when computing that. So the array can't be freed under us while we're
    // attached, but it can if this thread has detached and the resize races
    // with this function. To make that harmless in practice, load the array
    // like CPython does, then check that it wasn't replaced while we read
    // from it and that the instruction pointer really is inside the copy we
    // found, and give up if either check fails. Freed memory is very unlikely
    // to be unmapped within the few instructions this takes, so a garbage
    // read should be caught by those checks rather than crash.
    Py_ssize_t num_code_units = Py_SIZE(code);
    auto index = field<int32_t>(frame, layout.frame_tlbc_index);
    if (index > 0) {
        _PyCodeArray* array = __atomic_load_n(&code->co_tlbc, __ATOMIC_ACQUIRE);
        if (array && index < __atomic_load_n(&array->size, __ATOMIC_RELAXED)) {
            auto copy = reinterpret_cast<const CodeUnit*>(
                    __atomic_load_n(&array->entries[index], __ATOMIC_RELAXED));
            if (copy && __atomic_load_n(&code->co_tlbc, __ATOMIC_ACQUIRE) == array
                && pointsInto(instr, copy, num_code_units))
            {
                return copy;
            }
        }
    }
    // A frame can briefly point into another thread's copy after being
    // resumed on a different thread, so only trust the main copy if the
    // instruction pointer is inside it.
    return pointsInto(instr, main_copy, num_code_units) ? main_copy : nullptr;
#    else
    return main_copy;
#    endif
}
#endif

// Return the calling thread's thread state, or a null pointer if it has none
// or if it isn't safe to read.
PyThreadState*
walkableThreadState()
{
#if PY_VERSION_HEX >= 0x030D0000
    // PyThreadState_GetUnchecked returns the attached thread state, which is
    // NULL if this thread released the GIL. In that case fall back to the
    // thread state bound to this thread for the PyGILState API, which isn't
    // cleared on detach. During finalization, the thread states of threads
    // that aren't attached may be freed, so don't follow them at all.
    PyThreadState* tstate = PyThreadState_GetUnchecked();
    if (!tstate) {
        if (Py_IsFinalizing()) {
            return nullptr;
        }
        tstate = PyGILState_GetThisThreadState();
    }
    return tstate;
#else
    // Before 3.13 this needs internal APIs; see frame_layout_core.c.
    return memray_get_walkable_thread_state();
#endif
}

#if PY_VERSION_HEX < 0x030B0000
// Before 3.11, frames are PyFrameObjects linked by `f_back`, and every frame
// in the chain is complete and is its own entry frame.
size_t
walkFrames(const FrameLayout& layout, const void* frame, RawFrame* out, size_t max)
{
    size_t count = 0;
    while (frame) {
        if (count < max) {
            out[count].code = field<PyCodeObject*>(frame, layout.frame_executable);
            out[count].instruction_offset = field<int>(frame, layout.frame_lasti);
            out[count].is_entry_frame = true;
        }
        ++count;
        frame = field<const void*>(frame, layout.frame_previous);
    }
    return count;
}
#else
// From 3.11, frames are _PyInterpreterFrames linked by `previous`.
size_t
walkFrames(const FrameLayout& layout, const void* frame, RawFrame* out, size_t max)
{
    size_t count = 0;
    [[maybe_unused]] size_t steps = 0;
    while (frame) {
        // A sane chain is never this long; a longer one is probably a cycle
        // from reading a chain that's in the middle of being built.
        assert(++steps < (1u << 24));

        char owner = field<char>(frame, layout.frame_owner);
        const void* prev = field<const void*>(frame, layout.frame_previous);

        if (isNeverCompleteOwner(owner)) {
            frame = prev;  // An entry, trampoline, or interpreter frame.
            continue;
        }

        PyObject* executable = readExecutable(frame, layout.frame_executable);
        if (!executable || !PyCode_Check(executable)) {
            frame = prev;
            continue;
        }

        // This is `prev_instr` in 3.11 and 3.12, which points before the
        // instruction about to run, and `instr_ptr` from 3.13, which points
        // at it. CPython uses the same arithmetic on either one in
        // _PyFrame_IsIncomplete and PyFrame_GetLasti.
        auto code = reinterpret_cast<PyCodeObject*>(executable);
        auto instr = field<const CodeUnit*>(frame, layout.frame_instr_ptr);
        const CodeUnit* bytecode = frameBytecode(layout, frame, code, instr);

        // Equivalent to _PyFrame_IsIncomplete, which PyFrame_GetBack
        // and PyEval_GetFrame use to skip frames that haven't yet run their
        // setup instructions. If we couldn't find the bytecode, assume the
        // frame is complete, as it almost always is.
        if (bytecode && owner != FRAME_OWNED_BY_GENERATOR
            && reinterpret_cast<uintptr_t>(instr)
                       < reinterpret_cast<uintptr_t>(bytecode + code->_co_firsttraceable))
        {
            frame = prev;
            continue;
        }

        if (count < max) {
            // Equivalent to PyFrame_GetLasti and _PyFrame_IsEntryFrame.
            ptrdiff_t offset = -1;
            if (bytecode) {
                offset = static_cast<ptrdiff_t>(
                        reinterpret_cast<uintptr_t>(instr) - reinterpret_cast<uintptr_t>(bytecode));
            }
            out[count].code = code;
            out[count].instruction_offset = offset < 0 ? -1 : static_cast<int>(offset);
#    if PY_VERSION_HEX < 0x030C0000
            out[count].is_entry_frame = field<char>(frame, layout.frame_is_entry) != 0;
#    else
            out[count].is_entry_frame =
                    prev && isEntryFramePreviousOwner(field<char>(prev, layout.frame_owner));
#    endif
        }
        ++count;
        frame = prev;
    }
    return count;
}
#endif

}  // namespace

#if PY_VERSION_HEX >= 0x030D0000
const DebugOffsets*
findRuntimeDebugOffsets()
{
    // `_PyRuntime` is exported with PyAPI_DATA, and its first member is the
    // `_Py_DebugOffsets` table.
    return static_cast<const DebugOffsets*>(dlsym(RTLD_DEFAULT, "_PyRuntime"));
}

std::string
validateDebugOffsets(const DebugOffsets& table)
{
    if (memcmp(table.cookie, "xdebugpy", sizeof(table.cookie)) != 0) {
        return "the debug offsets table has the wrong cookie";
    }
    if ((table.version >> 16) != (PY_VERSION_HEX >> 16)) {
        return "the debug offsets table is for an unsupported Python version";
    }
#    ifdef Py_GIL_DISABLED
    const uint64_t free_threaded = 1;
#    else
    const uint64_t free_threaded = 0;
#    endif
    if (table.free_threaded != free_threaded) {
        return "the debug offsets table is for a different kind of Python build";
    }

    const auto& ts = table.thread_state;
    if (ts.size == 0 || ts.current_frame >= ts.size) {
        return "the debug offsets table has an invalid thread state layout";
    }

    const auto& frame = table.interpreter_frame;
    if (frame.size == 0 || frame.previous >= frame.size || frame.executable >= frame.size
        || frame.instr_ptr >= frame.size || frame.owner >= frame.size)
    {
        return "the debug offsets table has an invalid interpreter frame layout";
    }
#    if defined(Py_GIL_DISABLED) && PY_VERSION_HEX >= 0x030E0000
    if (frame.tlbc_index >= frame.size) {
        return "the debug offsets table has an invalid interpreter frame layout";
    }
#    endif

    return {};
}

DebugOffsets
loadDebugOffsets()
{
    static std::optional<DebugOffsets> s_validated;

    if (s_override) {
        std::string error = validateDebugOffsets(*s_override);
        if (!error.empty()) {
            throw std::runtime_error("Memray cannot track Python stacks: " + error);
        }
        return *s_override;
    }

    if (s_validated) {
        return *s_validated;
    }

    const DebugOffsets* table = findRuntimeDebugOffsets();
    if (!table) {
        throw std::runtime_error("Memray cannot track Python stacks: _PyRuntime not found");
    }

    std::string error = validateDebugOffsets(*table);
    if (!error.empty()) {
        throw std::runtime_error("Memray cannot track Python stacks: " + error);
    }

    s_validated = *table;
    return *s_validated;
}
#endif

void
setDebugOffsetsOverrideForTesting(const std::string& table)
{
#if PY_VERSION_HEX >= 0x030D0000
    if (table.empty()) {
        s_override.reset();
        return;
    }
    if (table.size() != sizeof(DebugOffsets)) {
        throw std::invalid_argument("debug offsets table has the wrong size");
    }
    DebugOffsets override_table;
    memcpy(&override_table, table.data(), sizeof(override_table));
    s_override = override_table;
#else
    (void)table;
    throw std::runtime_error("Python versions before 3.13 have no debug offsets table");
#endif
}

FrameLayout
loadFrameLayout()
{
    FrameLayout layout{};
#if PY_VERSION_HEX >= 0x030D0000
    DebugOffsets offsets = loadDebugOffsets();
    layout.thread_state_current_frame = offsets.thread_state.current_frame;
    layout.frame_previous = offsets.interpreter_frame.previous;
    layout.frame_executable = offsets.interpreter_frame.executable;
    layout.frame_instr_ptr = offsets.interpreter_frame.instr_ptr;
    layout.frame_owner = offsets.interpreter_frame.owner;
#    if defined(Py_GIL_DISABLED) && PY_VERSION_HEX >= 0x030E0000
    layout.frame_tlbc_index = offsets.interpreter_frame.tlbc_index;
#    endif
#else
    memray_get_core_frame_layout(&layout);
#endif
    return layout;
}

size_t
walkCurrentThread(const FrameLayout& layout, RawFrame* out, size_t max)
{
    // Only the thread that owns a frame chain ever changes it, and the owning
    // thread is the one running this function, so nothing can change the
    // chain while we walk it, whether or not we hold the GIL.
    PyThreadState* tstate = walkableThreadState();
    if (!tstate) {
        return 0;  // This thread has no Python state, or it can't be read.
    }

    const void* frame = field<const void*>(tstate, layout.thread_state_current_frame);
#if PY_VERSION_HEX >= 0x030B0000 && PY_VERSION_HEX < 0x030D0000
    // In 3.11 and 3.12 that's the current _PyCFrame, which holds the frame.
    if (frame) {
        frame = field<const void*>(frame, layout.cframe_current_frame);
    }
#endif
    return walkFrames(layout, frame, out, max);
}

std::string
unicodeToUtf8(PyObject* str)
{
    // Strings are immutable, and these fields never change after creation,
    // so reading them is safe without the GIL.
    std::string ret;
    if (!str || !PyUnicode_Check(str)) {
        return ret;
    }
#if PY_VERSION_HEX < 0x030C0000
    // Legacy strings that haven't been made ready have no data to read.
    // Code objects' strings are always ready.
    if (!PyUnicode_IS_READY(str)) {
        return ret;
    }
#endif

    Py_ssize_t length = PyUnicode_GET_LENGTH(str);
    if (PyUnicode_IS_ASCII(str)) {
        ret.assign(static_cast<const char*>(PyUnicode_DATA(str)), length);
        return ret;
    }

    int kind = PyUnicode_KIND(str);
    const void* data = PyUnicode_DATA(str);
    ret.reserve(length);
    for (Py_ssize_t i = 0; i < length; ++i) {
        appendUtf8(ret, PyUnicode_READ(kind, data, i));
    }
    return ret;
}

tracking_api::CodeObjectInfo
readCodeObjectInfo(PyCodeObject* code)
{
    size_t linetable_size = 0;
    const char* linetable = compat::codeGetLinetable(code, &linetable_size);
    return tracking_api::CodeObjectInfo{
            unicodeToUtf8(code->co_name),
            unicodeToUtf8(code->co_filename),
            linetable ? std::string(linetable, linetable_size) : std::string(),
            code->co_firstlineno};
}

}  // namespace memray::frame_walker
