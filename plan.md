# Plan: replace the shadow stack with a direct frame walk (3.13 proof of concept)

## Goal

Stop mirroring each thread's Python stack from profile events. Instead, at
each allocation, the allocating thread reads its own interpreter frame chain.
This holds with or without the GIL. The shadow stack is the source of the
stale-frame bugs and use-after-free crashes in #1063: the mirror drifts from
the real stack whenever a frame starts or ends without a profile event.

Scope of this proof of concept:

- Correctness only. Performance and optimization are out of scope; a full
  walk on every allocation is fine.
- CPython 3.13 only, with the GIL. Every other version, and the free-threaded
  3.13t build, is dropped for now. 3.13 is the first version with
  `_Py_DebugOffsets`, which the walker uses so that memray never has to define
  `Py_BUILD_CORE` or include internal headers.
- No file format or reader changes. We keep writing `FramePush`/`FramePop`
  records.

## Why reading without the GIL is safe

A thread's interpreter frame chain is changed only by the thread that owns it.
The chain starts at `tstate->current_frame` and follows `previous`. When our
`malloc` hook runs, the owning thread is the one running the hook, so nothing
can change the chain while we read it. If the thread released the GIL first,
its frames are paused in a C call and stay alive until it gets the GIL back.
That can't happen while it's inside our hook. Each frame holds a strong
reference to its `f_executable` (its code object), so code objects stay alive
during the walk too.

Nothing we keep afterwards points at a frame. The only state carried between
allocations is plain data: code object ids and instruction offsets.

## Design

### 1. Where structure layouts come from

Two sources, and no internal headers:

- **`_PyInterpreterFrame` is internal**, so its layout comes from
  `_Py_DebugOffsets`. For consistency, `PyThreadState.current_frame` is read
  through it too, even though that field is in the public
  `cpython/pystate.h`.
- **Code, `str` and `bytes` objects are defined in public, non-limited
  headers.** These are `PyCodeObject` in `cpython/code.h`, the `PyUnicode_*`
  macros, and `PyBytes_AS_STRING`/`PyBytes_GET_SIZE`. memray already uses
  these. Use them directly. This also gets us `PyCodeObject._co_firsttraceable`,
  which `_Py_DebugOffsets` doesn't include and which the incomplete-frame check
  needs.

#### Loading the offsets table

The table is the first member of `_PyRuntime` (`_PyRuntimeState.debug_offsets`).
`_PyRuntime` is exported with `PyAPI_DATA`, so we can find it without
including its declaration:

- Find the symbol with `dlsym(RTLD_DEFAULT, "_PyRuntime")`. That works whether
  libpython is shared or statically linked into an executable that exports its
  symbols, which extension modules already depend on. An alternative is to
  declare it as an opaque `extern "C" char _PyRuntime;`.
- Define our own `DebugOffsets313` struct in a new header,
  `src/memray/_memray/debug_offsets.h`. It must match the field order of
  3.13's `_Py_DebugOffsets` in `Include/internal/pycore_runtime.h` exactly, at
  least up to `code_object`. This copies the table's layout, not CPython's
  struct layouts. It's the same thing out-of-process tools do.
- At first tracker start, validate the table once and keep a copy:
  - `cookie` equals `"xdebugpy"`
  - `version >> 16 == 0x030D` (the runtime is 3.13)
  - `free_threaded == 0`
  - `interpreter_frame.size` is non-zero and each offset we use is smaller
    than its section's `size`

  If any check fails, refuse to start tracking with a clear error.

The frame-owner values aren't in the table. Hardcode them per version, with
the version check above guarding them. For 3.13: `FRAME_OWNED_BY_THREAD = 0`,
`FRAME_OWNED_BY_GENERATOR = 1`, `FRAME_OWNED_BY_FRAME_OBJECT = 2`,
`FRAME_OWNED_BY_CSTACK = 3`.

### 2. Frame walker (new, C++)

There are no internal headers, so this can stay C++. Put it in new files
`src/memray/_memray/frame_walker.cpp` and `frame_walker.h`:

```c++
struct RawFrame {
    PyCodeObject* code;      // valid only while the walk is in progress
    int instruction_offset;  // same units as PyFrame_GetLasti: bytes, or -1
    bool is_entry_frame;     // previous frame is owned by the C stack
};

// Walk the calling thread's frames, innermost first. Must not allocate or
// touch refcounts. Returns the number of frames written, at most max.
size_t walkCurrentThread(const DebugOffsets313& off, RawFrame* out, size_t max);
```

Below, `field<T>(ptr, offset)` means reading a `T` at `(char*)ptr + offset`.

1. `tstate = PyGILState_GetThisThreadState()`. Unlike `_PyThreadState_GET()`,
   this is still valid after the thread releases the GIL: in 3.13
   `_Py_tss_tstate` is cleared on detach, but the GIL-state TSS binding isn't.
   If it returns `NULL`, the thread has no Python state: return 0.
2. Start at `f = field<void*>(tstate, off.thread_state.current_frame)`. For
   each frame, read:
   - `owner = field<char>(f, off.interpreter_frame.owner)`
   - `prev = field<void*>(f, off.interpreter_frame.previous)`
   - `exec = field<PyObject*>(f, off.interpreter_frame.executable)`
   - `instr = field<_Py_CODEUNIT*>(f, off.interpreter_frame.instr_ptr)`

   Then:
   - Skip the frame if `owner == FRAME_OWNED_BY_CSTACK`. These are entry or
     trampoline frames, whose `f_executable` is `Py_None`.
   - Skip it if `exec` isn't a code object (`!PyCode_Check(exec)`). Reading
     `ob_type` of a live object is fine without the GIL.
   - Skip it if it's incomplete, using 3.13's `_PyFrame_IsIncomplete`, built
     from public fields: `owner != FRAME_OWNED_BY_GENERATOR && instr <
     (_Py_CODEUNIT*)code->co_code_adaptive + code->_co_firsttraceable`. This
     matches `PyFrame_GetBack`, which skips incomplete frames anywhere in the
     chain.
   - Otherwise record:
     - `code = (PyCodeObject*)exec`
     - offset `(char*)instr - code->co_code_adaptive`, or `-1` if negative.
       This is the same as `PyFrame_GetLasti`.
     - `is_entry = prev && field<char>(prev, off.interpreter_frame.owner) ==
       FRAME_OWNED_BY_CSTACK`. This is the same as `_PyFrame_IsEntryFrame`.
3. Debug builds only: assert the frame count stays within a sane limit, to
   catch a half-built chain.

The result must equal what memray builds today from `PyEval_GetFrame()` plus
`compat::frameGetBack()` (see `populateShadowStack`). The test plan checks this
directly.

### 3. Reading code object info without the GIL

`LazilyEmittedFrame`'s constructor currently fills in `CodeObject` with the GIL
held, using `PyUnicode_AsUTF8`. That function may allocate a UTF-8 cache, so it
can't be called without the GIL. Replace it with a reader that doesn't need
the GIL, used only when the cache misses:

- `co_name` and `co_filename`: build the UTF-8 `std::string` ourselves from
  `PyUnicode_KIND`, `PyUnicode_DATA` and `PyUnicode_GET_LENGTH`. Strings never
  change and the code object keeps them alive, so reading them is safe. Use
  the ASCII data directly when `PyUnicode_IS_ASCII`.
- `co_linetable`: `PyBytes_AS_STRING` and `PyBytes_GET_SIZE`. This is
  `compat::codeGetLinetable` on 3.13; confirm it doesn't allocate.
- `co_firstlineno`: read directly.

Change `CodeObject` to own its strings (`std::string`) instead of `const char*`,
or add a variant that does. Split `Tracker::registerCodeObject` into "look up
the id by pointer" and "register", so the info is only built on a miss.

Address reuse is already handled. The cache is keyed by `PyCodeObject*`, and
the `PyCode_AddWatcher` callback (`Tracker::Tracker`) evicts entries on
`PY_CODE_EVENT_DESTROY` while holding the Tracker lock. A code object can't
be destroyed during a walk that reaches it. There's one existing gap that
matters more now: see Risks.

### 4. Turning walks into push/pop records

Replace `PythonStackTracker` with a much smaller per-thread object. Call it
`PythonStackEmitter`. It holds:

- `std::vector<Frame> d_last_emitted`: the stack the reader currently believes
  this thread has, outermost first. Each entry is `Frame{code_object_id,
  instruction_offset, is_entry_frame}`, which is plain data with no frame
  pointers.
- `d_tracker_generation`. If it doesn't match `s_tracker_generation`, a new
  tracker started, so clear `d_last_emitted` without emitting pops: the new
  output file starts with no stack.

`emitCurrentStack(Tracker&)` replaces both `get()` and
`emitPendingPushesAndPops()`, and runs with the Tracker lock held:

1. Walk the frames into a buffer and reverse it to outermost first.
2. Turn each raw frame into a `Frame`: look up or register the code object;
   set `is_entry_frame = !native_traces || raw.is_entry_frame`, as
   `LazilyEmittedFrame` does now.
3. Find the longest common prefix with `d_last_emitted`, comparing all three
   fields.
4. Write `popFrames(d_last_emitted.size() - prefix)` if that's non-zero, then
   `pushFrame` for each new frame after the prefix. On write failure, stop and
   leave the state matching what was actually written. The writer already
   deactivates the tracker on failure.
5. Store the new stack in `d_last_emitted`.

Two frames compare equal if they have the same code id, offset and entry flag,
even if they're different calls. That's fine: the reported stack is identical.

Keep the thread-teardown handling described in the long comment above
`PythonStackTracker` (`tracking_api.cpp`): create the TLS vector in exactly one
place and guard access with a pointer that the vector's destructor clears.

Callers: `trackAllocationImpl` and `trackObjectImpl` call
`PythonStackEmitter::get().emitCurrentStack(*this)` where they call
`emitPendingPushesAndPops()` today. `trackDeallocationImpl` doesn't need a
stack and stays as it is.

### 5. What gets deleted

`src/memray/_memray/tracking_api.cpp`:
- `PythonStackTracker`, including `LazilyEmittedFrame`,
  `s_initial_stack_by_thread`, `recordAllStacks`, `pythonFrameToStack`,
  `reloadStackIfTrackerChanged`, `populateShadowStack`, `handleTraceEvent`,
  `pushPythonFrame`, `popPythonFrame`, `installProfileHooks`,
  `removeProfileHooks`, `installGreenletTraceFunctionIfNeeded`,
  `handleGreenletSwitch`, `s_greenlet_tracking_enabled`
- `PyTraceFunction`, `install_trace_function`,
  `Tracker::beginTrackingGreenlets`, `Tracker::handleGreenletSwitch`
- `StopTheWorldGuard`, and the stop-the-world plus `recordAllStacks` steps in
  `createTracker` and `childFork`. Keep the `s_tracker_generation++`.
- The `s_extra_index` / `_PyCode_*Extra` branch for versions before 3.12

`src/memray/_memray/tracking_api.h` and `tracking_api.pxd`: the matching
declarations.

`src/memray/_memray.pyx`:
- `threading.setprofile(start_thread_trace)`, plus saving and restoring
  `sys.setprofile` / `threading.setprofile` in `__enter__` and `__exit__`
- `start_thread_trace`, `greenlet_trace_function`, `print_greenlet_warning`
- the `beginTrackingGreenlets()` call

`src/memray/_memray/compat.h` / `compat.cpp`: anything left unused, such as
`setprofileAllThreads`, `stopTheWorld` and the frame helpers.
`computeMainTidSkip` still uses `PyEval_GetFrame` with the GIL held; keep it.

### 6. Restricting to 3.13

- `pyproject.toml`: `requires-python = ">=3.13,<3.14"`; cibuildwheel `build`
  set to `cp313-*`, with no `t` (free-threaded) builds
- `frame_walker.cpp`: if `PY_VERSION_HEX` isn't 3.13, or `Py_GIL_DISABLED` is
  defined, then `#error`
- the runtime table check in section 1 catches a mismatch between the
  version memray was built for and the version it runs on
- CI workflows: reduce the test matrices to 3.13
- Leave the `PY_VERSION_HEX` branches elsewhere alone unless they get in the
  way. Clean them up when adding versions back.

## Intended behaviour changes

1. **Greenlets no longer get their own thread IDs.** That needed the switch
   hook. With the walk, each allocation reports the running greenlet's real
   stack under the OS thread's ID. Stacks are correct; they just aren't split
   by greenlet. Whether to bring back the split is an open question below.
2. **Stacks stay correct after `sys.setprofile(None)`.** Today they become
   empty, to avoid reading freed frames.
3. **memray no longer uses the profile hook.** It can run alongside cProfile,
   profiling debuggers and coverage tools that use it.
4. **Frames of `sys.settrace` callbacks show up in stacks** when an allocation
   happens inside one. That's accurate: they really are running.
5. **Starting a tracker no longer captures other threads' stacks or stops the
   world.** Each thread reads its own stack when it allocates.

## Test plan

All tests run on 3.13 with the GIL.

1. **Check the walker against the public API.** Add a test-only helper in
   `_memray_test_utils.pyx` that runs `walkCurrentThread` with the GIL held
   and returns `(co_name, lineno)` pairs. Compare it with the
   `sys._getframe()` / `f_back` chain in:
   - plain and recursive calls
   - generators, coroutines and async generators
   - `yield from` and `gen.throw()` delegation chains, the same shapes as the
     #1063 tests
   - exceptions in flight, `finally` blocks
   - class bodies, decorators, lambdas
   - threads, and inside greenlets
2. **Check the offsets table.** A unit test compares each offset we use with
   the values implied by the public headers: `offsetof(PyThreadState,
   current_frame)` must equal `thread_state.current_frame`. Also test that
   tracking refuses to start if the table check fails, by faking a bad table
   through a test hook.
3. **Existing suite.** Run all of `tests/` and sort the failures:
   - **Expected to change:**
     - `test_tracing.py::test_allocation_after_unsetting_profile_function`
       and `test_allocation_in_thread_after_unsetting_profile_function`: flip
       to expect the full stack
     - `test_profile_function_is_restored_after_tracking`: drop, or replace
       with "memray never touches the profile function"
     - the TID assertions in `test_greenlet.py`
   - **Must still pass unchanged:**
     - `test_allocation_in_thread_started_before_tracking_starts`
     - `test_allocation_in_thread_before_reacquiring_gil_after_tracking_starts`
     - `test_thread_surviving_multiple_trackers*`
     - `test_cython_frame_in_pre_existing_thread_stack*`
     - `test_large_number_of_frame_pops_between_subsequent_allocations`
     - the hybrid-stack tests in `test_native_tracking.py`
4. **#1063 regression tests.** Add the reporter's three tests, with TID
   assertions removed. All three should pass under `-X dev`, including
   `test_greenlet_suspended_when_tracking_starts`. Turn the issue's repro
   script into a test that asserts `_run_once` appears only once.
5. **Allocations without the GIL.** Add `_test_utils` helpers that allocate
   with the GIL released (`with nogil:`), called from:
   - a deep stack
   - a generator
   - a greenlet
   - a thread started before tracking

   Check the reported stacks against the expected functions and lines.
6. **Stress runs**, under `PYTHONMALLOC=debug` and ideally an ASan build of
   memray:
   - the #1063 stress pattern: many event-loop threads, cancelled
     `greenlet_spawn` calls, and a tracker opened and closed every second
   - threads constantly starting and exiting while allocating, to exercise
     the TLS teardown path
   - `os.fork()` with `follow_fork=True`

## Risks and things to check

- **Copying the table's layout.** Our `DebugOffsets313` must match 3.13's
  `_Py_DebugOffsets` field for field. The table was designed for external
  tools, so it should only change between minor versions. Still, diff it
  against the `Include/internal/pycore_runtime.h` of the newest 3.13 patch
  release, and keep the size check in section 1.
- **Owner values and `_co_firsttraceable`.** Neither is in the table: the
  owner values are hardcoded constants, and `_co_firsttraceable` comes from
  the public `PyCodeObject`. Both are version-specific, guarded by the version
  check.
- **Allocations from inside the interpreter while the frame chain is
  half-built.** Our hook can run during frame push or teardown, for example
  when CPython allocates a new frame-stack chunk. It's the same thread, so the
  reads don't race, but `current_frame` and the `previous` links might briefly
  disagree. Check the 3.13 `_PyEvalFramePushAndInit`, `CALL` and
  `_PyEval_FrameClearAndPop` paths to see whether an allocation can happen
  between linking a frame and initializing it. The debug assertions in the
  walker are there to catch this.
- **Greenlet switches in progress.** Greenlet allocates (`g_save`) while it
  copies C stack slices and swaps thread-state fields. Entry frames
  (`FRAME_OWNED_BY_CSTACK`) live on the C stack. Make sure an allocation in
  the middle of a switch can't follow `current_frame` into a C stack region
  greenlet has already overwritten. If it can, read only `owner` and
  `previous` from such frames, or detect a switch in progress.
- **Missed code object destruction.** The code watcher callback skips
  eviction when `RecursionGuard::isActive()`. If a code object dies while the
  guard is on (for example during `set_os_name_wrapper` in `_memray.pyx`,
  which runs Python code under the guard), the cache keeps a stale entry. A
  new code object at the same address would then be reported with the old
  name. This bug exists today, but every allocation now looks up code by
  pointer. Fix it by evicting even when the guard is on, as long as this
  thread doesn't hold the Tracker lock.
- **`PyGILState_GetThisThreadState()` edge cases:** sub-interpreters, threads
  with several thread states, and the window during `PyThreadState_Delete`.
  Make sure no allocation follows a thread-state pointer after it has been
  freed.

## Steps

1. Restrict the build and CI to 3.13 (section 6). Confirm the existing suite
   passes before changing anything else.
2. Add `debug_offsets.h` and the table loading and checks, `frame_walker.cpp`,
   and the code-object reader that doesn't need the GIL, with tests 1–2. The
   tracker doesn't use them yet.
3. **Run both side by side.** In a debug build, call the walker at every
   allocation next to the shadow stack and log any disagreement. Run the
   suite and the #1063 repros. The only disagreements should be the known
   shadow-stack bugs.
4. Switch emission to `PythonStackEmitter` and delete the shadow stack,
   profile hooks and greenlet hooks (sections 4 and 5).
5. Update the tests with expected changes; add the new tests (test plan 3–5).
6. Stress testing (test plan 6). Then decide on the open questions.

## Open questions, for after the proof of concept

- **Separate thread IDs per greenlet:** bring this back? It would need the
  switch hook again, used only to change the TID, plus a saved
  `d_last_emitted` for each greenlet ID. No frame pointers would be stored.
- **Adding versions back:**
  - **3.14 and later:** `_Py_DebugOffsets` exists but its layout differs, so
    each version needs its own struct copy. 3.14 also renumbers the frame
    owners (adds `FRAME_OWNED_BY_INTERPRETER = 3`, moves
    `FRAME_OWNED_BY_CSTACK` to 4) and makes `f_executable` a tagged
    `_PyStackRef`.
  - **Free-threaded builds:** tagged `f_executable`, plus per-thread copies of
    the bytecode (`tlbc_index`, which is in the table).
  - **3.12 and earlier have no `_Py_DebugOffsets`.** Supporting them needs
    either hand-written offset tables in the same shape, or internal headers.
    3.12 and 3.11 use `tstate->cframe->current_frame` and `prev_instr`; 3.10
    and earlier use the `PyFrameObject` chain via `f_back` and `f_lasti`.
- **Keep skipping incomplete frames?** Address this once the proof of
  concept passes all the tests. The walker reads `_co_firsttraceable` to
  skip frames still running their setup instructions before the first
  `RESUME`: `MAKE_CELL`, `COPY_FREE_VARS`, or `RETURN_GENERATOR` +
  `POP_TOP`. That matches `sys._getframe()` and today's stacks, which keeps
  the step 3 comparison clean. Dropping the check is safe for memory. Its
  only effect is attribution: allocations during that setup move from the
  caller to the function being started, at its `def` line. In particular,
  every generator, coroutine and async generator object would be reported
  as allocated inside its own function. Tachyon reports these frames (it
  filters only by owner). Dropping the check would also remove the walker's
  last read of a private `PyCodeObject` field.
- **Performance:** per-allocation cost of a full walk, and whether to use
  3.15's `last_profiled_frame` anchor (as Tachyon does) or memray's own
  "unchanged below this frame" detection.
