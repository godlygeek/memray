#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <cassert>
#include <cerrno>
#include <stdio.h>

#ifdef __linux__
#    include <link.h>
#elif defined(__APPLE__)
#    include "macho_utils.h"
#    include <mach/mach.h>
#    include <mach/task.h>
#endif

#include <algorithm>
#include <mutex>
#include <type_traits>
#include <unistd.h>

#include "compat.h"
#include "exceptions.h"
#include "frame_walker.h"
#include "hooks.h"
#include "record_writer.h"
#include "records.h"
#include "tracking_api.h"

using namespace memray::exception;
using namespace std::chrono_literals;

namespace {

#ifdef __linux__
std::string
get_executable()
{
    char buff[PATH_MAX + 1];
    ssize_t len = ::readlink("/proc/self/exe", buff, sizeof(buff));
    if (len > PATH_MAX) {
        throw std::runtime_error("Path to executable is more than PATH_MAX bytes");
    } else if (len == -1) {
        throw std::runtime_error("Could not determine executable path");
    }
    return std::string(buff, len);
}

static bool
starts_with(const std::string_view& haystack, const std::string_view& needle)
{
    return haystack.compare(0, needle.size(), needle) == 0;
}

static bool
parse_proc_status_value_in_kb(
        const std::string& proc_status,
        const std::string_view& field_name,
        size_t* value_in_kb)
{
    size_t line_start = 0;
    while (line_start < proc_status.size()) {
        size_t line_end = proc_status.find('\n', line_start);
        if (line_end == std::string::npos) {
            line_end = proc_status.size();
        }

        std::string_view line = std::string_view(proc_status).substr(line_start, line_end - line_start);
        if (starts_with(line, field_name)
            && sscanf(proc_status.c_str() + line_start + field_name.size(), "%zu kB", value_in_kb) == 1)
        {
            return true;
        }

        line_start = line_end + 1;
    }
    return false;
}
#endif

}  // namespace

namespace memray::tracking_api {

bool
getRSSFromProcStatus(const std::string& proc_status, size_t* rss_in_bytes)
{
#ifdef __linux__
    size_t rss_in_kb;
    if (!parse_proc_status_value_in_kb(proc_status, "VmRSS:", &rss_in_kb)) {
        return false;
    }

    size_t hugetlb_in_kb;
    if (parse_proc_status_value_in_kb(proc_status, "HugetlbPages:", &hugetlb_in_kb)) {
        rss_in_kb += hugetlb_in_kb;
    }

    *rss_in_bytes = rss_in_kb * 1024;
    return true;
#else
    return false;
#endif
}

#ifdef __linux__
MEMRAY_FAST_TLS thread_local bool RecursionGuard::_isActive = false;
#else
pthread_key_t RecursionGuard::isActiveKey;
#endif

static inline thread_id_t
generate_next_tid()
{
    static std::atomic<thread_id_t> s_tid_counter = 0;
    return ++s_tid_counter;
}

MEMRAY_FAST_TLS thread_local thread_id_t t_tid = generate_next_tid();

static inline thread_id_t
thread_id()
{
    return t_tid;
}

// Tracker interface

static bool s_native_tracking_enabled{false};

// Python stack emission
//
// At each allocation, the allocating thread walks its own interpreter frame
// chain (see frame_walker.h) and emits FramePush and FramePop records to bring
// the stack that the reader believes this thread has in line with the walk.
// Nothing kept between allocations points at a frame or code object: the only
// state carried over is the last emitted stack, which is plain data.
//
// If a TLS variable has not been constructed, accessing it will cause it to be
// constructed. That's normally great, but we need to prevent that from
// happening unexpectedly for the TLS vectors owned by this class.
//
// Methods of this class can be called during thread teardown. It's possible
// that, after the TLS vectors for a dying thread have already been destroyed,
// libpthread makes a call to malloc() that calls into our Tracker, and if it
// does, we must prevent it touching the vectors again and re-constructing
// them. Otherwise, they would be re-constructed immediately but their
// destructor would be added to this thread's list of finalizers after all the
// finalizers for the thread already ran. If that happens, the vectors will be
// free()d before their destructor runs. Worse, their destructor will remain on
// the list of finalizers for the current thread's pthread struct, and will
// later be run on that already free()d memory if this thread's pthread struct
// is ever reused. When that happens it tends to cause heap corruption, because
// other vectors are placed at the same location as the original ones, and the
// vector destructors run twice on them (once for the newly created vectors,
// and once for the vectors that had been created before the thread died and
// the pthread struct was reused).
//
// To prevent that, we create the vectors in one method, getBuffers. It sets
// `d_buffers` to point to the TLS vectors when they're created, and they set
// it back to a null pointer and set `d_buffers_destroyed` when they're
// destroyed, after which getBuffers never creates them again.
//
// This can result in this class being constructed during thread teardown, but
// that doesn't cause the same problem because it has a trivial destructor.
class PythonStackEmitter
{
  public:
    static PythonStackEmitter& get();

    // Bring the emitted stack for this thread in line with its current
    // Python stack. Must be called with the Tracker lock held.
    void emitCurrentStack(Tracker& tracker, const frame_walker::FrameLayout& layout);

    // Incremented each time a new Tracker is installed.
    static std::atomic<unsigned int> s_tracker_generation;

  private:
    PythonStackEmitter() = default;

    struct Buffers
    {
        // The stack the reader currently believes this thread has, outermost first.
        std::vector<Frame> last_emitted;

        // Scratch space, kept to avoid allocating on every call. `raw` is
        // only used for stacks too deep for the walk's on-stack buffer.
        std::vector<Frame> current;
        std::vector<frame_walker::RawFrame> raw;
    };

    Buffers* getBuffers();

    Buffers* d_buffers{};
    bool d_buffers_destroyed{};
    // The size of `last_emitted`, which outlives the vector itself, so that
    // we can still pop the emitted frames after the vector is destroyed.
    uint32_t d_emitted_depth{};
    unsigned int d_tracker_generation{};
};

std::atomic<unsigned int> PythonStackEmitter::s_tracker_generation;

PythonStackEmitter&
PythonStackEmitter::get()
{
    // See giant comment above.
    static_assert(std::is_trivially_destructible<PythonStackEmitter>::value);
    MEMRAY_FAST_TLS thread_local PythonStackEmitter t_python_stack_emitter;
    return t_python_stack_emitter;
}

PythonStackEmitter::Buffers*
PythonStackEmitter::getBuffers()
{
    if (d_buffers || d_buffers_destroyed) {
        return d_buffers;
    }

    struct BuffersCreator
    {
        Buffers buffers;

        BuffersCreator()
        {
            const size_t INITIAL_PYTHON_STACK_FRAMES = 128;
            buffers.last_emitted.reserve(INITIAL_PYTHON_STACK_FRAMES);
            buffers.current.reserve(INITIAL_PYTHON_STACK_FRAMES);
            PythonStackEmitter::get().d_buffers = &buffers;
        }
        ~BuffersCreator()
        {
            PythonStackEmitter::get().d_buffers = nullptr;
            PythonStackEmitter::get().d_buffers_destroyed = true;
        }
    };

    MEMRAY_FAST_TLS static thread_local BuffersCreator t_buffers_creator;
    assert(d_buffers || d_buffers_destroyed);  // The constructor sets d_buffers.
    return d_buffers;
}

void
PythonStackEmitter::emitCurrentStack(Tracker& tracker, const frame_walker::FrameLayout& layout)
{
    if (d_tracker_generation != s_tracker_generation) {
        // A new Tracker was installed. Its output starts with no stack for
        // this thread, so forget what we emitted without emitting pops.
        d_tracker_generation = s_tracker_generation;
        d_emitted_depth = 0;
        if (d_buffers) {
            d_buffers->last_emitted.clear();
        }
    }

    // Walk into a small buffer first, so that threads without Python frames
    // that never emitted any never need to create the TLS buffers.
    frame_walker::RawFrame small_buffer[32];
    size_t num_frames = frame_walker::walkCurrentThread(layout, small_buffer, std::size(small_buffer));
    if (num_frames == 0 && d_emitted_depth == 0) {
        return;
    }

    Buffers* buffers = getBuffers();
    if (!buffers) {
        // This thread's TLS has been destroyed, so it's exiting and can't be
        // running Python code. Pop whatever we emitted before.
        if (d_emitted_depth && tracker.popFrames(d_emitted_depth)) {
            d_emitted_depth = 0;
        }
        return;
    }

    const frame_walker::RawFrame* raw = small_buffer;
    if (num_frames > std::size(small_buffer)) {
        auto& big_buffer = buffers->raw;
        do {
            big_buffer.resize(num_frames);
        } while ((num_frames =
                          frame_walker::walkCurrentThread(layout, big_buffer.data(), big_buffer.size()))
                 > big_buffer.size());
        raw = big_buffer.data();
    }

    // The walk is innermost first; build the stack outermost first.
    auto& current = buffers->current;
    current.clear();
    for (size_t i = num_frames; i-- > 0;) {
        PyCodeObject* code = raw[i].code;
        code_object_id_t code_id = tracker.lookupCodeObject(code);
        if (!code_id) {
            code_id = tracker.registerCodeObject(code, frame_walker::readCodeObjectInfo(code));
        }
        // If native tracking is not enabled, treat every frame as an entry
        // frame. It doesn't matter to the reader, and is more efficient.
        bool is_entry_frame = !s_native_tracking_enabled || raw[i].is_entry_frame;
        current.push_back(Frame{code_id, raw[i].instruction_offset, is_entry_frame});
    }
    tracker.addSkippedFramesIfNeeded(current);

    // Two frames compare equal if they have the same code object, offset,
    // and entry flag, even if they're different calls. That's fine, as the
    // reported stack is identical.
    auto& last_emitted = buffers->last_emitted;
    size_t common =
            std::mismatch(last_emitted.begin(), last_emitted.end(), current.begin(), current.end()).first
            - last_emitted.begin();

    if (common < last_emitted.size()) {
        if (!tracker.popFrames(last_emitted.size() - common)) {
            return;
        }
        last_emitted.resize(common);
        d_emitted_depth = common;
    }

    for (size_t i = common; i < current.size(); ++i) {
        if (!tracker.pushFrame(current[i])) {
            return;
        }
        last_emitted.push_back(current[i]);
        d_emitted_depth = last_emitted.size();
    }
}

std::unique_ptr<TrackerMutex> Tracker::s_mutex(new TrackerMutex);
pthread_key_t Tracker::s_native_unwind_vector_key;
std::unique_ptr<Tracker> Tracker::s_instance_owner;
std::atomic<Tracker*> Tracker::s_instance = nullptr;

#if PY_VERSION_HEX < 0x030C0000
static destructor s_original_code_dealloc;
#endif

void
Tracker::forgetDestroyedCodeObject(PyCodeObject* code)
{
    if (!Tracker::isActive()) {
        return;
    }
    // Evict even if the recursion guard is active (for instance, when a code
    // object dies while we run Python code under the guard). A stale entry
    // would make a new code object at the same address be reported with the
    // old one's details.
    RecursionGuard guard;

    if (s_mutex->heldByCurrentThread()) {
        Tracker* tracker = Tracker::getTracker();
        if (tracker) {
            tracker->forgetCodeObject(code);
        }
        return;
    }

    std::unique_lock lock(*s_mutex);
    Tracker* tracker = Tracker::getTracker();
    if (tracker) {
        tracker->forgetCodeObject(code);
    }
}

Tracker::Tracker(
        std::unique_ptr<RecordWriter> record_writer,
        bool native_traces,
        unsigned int memory_interval,
        bool follow_fork,
        bool trace_python_allocators,
        bool reference_tracking)
: d_frame_layout(frame_walker::loadFrameLayout())
, d_writer(std::move(record_writer))
, d_unwind_native_frames(native_traces)
, d_memory_interval(memory_interval)
, d_follow_fork(follow_fork)
, d_trace_python_allocators(trace_python_allocators)
, d_reference_tracking(reference_tracking)
{
    static std::once_flag once;
    call_once(once, [] {
        // We use the pthread TLS API for this vector because we must be able
        // to re-create it while TLS destructors are running (a destructor can
        // call malloc, hitting our malloc hook). POSIX guarantees multiple
        // rounds of TLS destruction if destructors call pthread_setspecific.
        // Note: If this raises an exception, the call_once can be retried.
        if (0
            != pthread_key_create(
                    &s_native_unwind_vector_key,
                    [](void* data) { delete static_cast<std::vector<NativeTrace::ip_t>*>(data); }))
        {
            throw std::runtime_error{"Failed to create pthread key"};
        }

        hooks::ensureAllHooksAreValid();
        NativeTrace::setup();

#if PY_VERSION_HEX >= 0x030C0000
        PyCode_AddWatcher([](PyCodeEvent event, PyCodeObject* code) {
            if (event == PY_CODE_EVENT_DESTROY) {
                forgetDestroyedCodeObject(code);
            }
            return 0;
        });
#else
        // There are no code watchers before 3.12, so wrap the code type's
        // deallocator instead. It can't be subclassed, so this sees every
        // code object's destruction.
        s_original_code_dealloc = PyCode_Type.tp_dealloc;
        PyCode_Type.tp_dealloc = [](PyObject* code) {
            forgetDestroyedCodeObject(reinterpret_cast<PyCodeObject*>(code));
            s_original_code_dealloc(code);
        };
#endif
    });

    hooks::ensureArrowHooksAreValid();

    size_t main_tid_skip = computeMainTidSkip();
    d_writer->setMainTidAndSkippedFrames(thread_id(), main_tid_skip);
    if (!d_writer->writeHeader(false)) {
        throw IoError{"Failed to write output header: " + std::string(strerror(errno))};
    }
    recordMainThreadSkippedFrames(main_tid_skip);

    RecursionGuard guard;
    updateModuleCacheImpl();

    s_native_tracking_enabled = native_traces;
    if (d_reference_tracking) {
        registerReferenceTrackingHooks();
    }
    if (d_trace_python_allocators) {
        registerPymallocHooks();
    }
    d_background_thread = std::make_unique<BackgroundThread>(d_writer, memory_interval);
    d_background_thread->start();

    d_patcher.overwrite_symbols();
}

Tracker::~Tracker()
{
    RecursionGuard guard;
    tracking_api::Tracker::deactivate();

    s_native_tracking_enabled = false;
    d_background_thread->stop();

    {
        std::scoped_lock lock(*s_mutex);
        d_patcher.restore_symbols();
    }

    if (Py_IsInitialized() && !compat::isPythonFinalizing()) {
        PyGILState_STATE gstate;
        gstate = PyGILState_Ensure();

        if (d_reference_tracking) {
            std::scoped_lock lock(*s_mutex);
            unregisterReferenceTrackingHooks();
        }

        if (d_trace_python_allocators) {
            std::scoped_lock lock(*s_mutex);
            unregisterPymallocHooks();
        }

        PyGILState_Release(gstate);
    }

    std::scoped_lock lock(*s_mutex);
    d_tracked_objects.clear();
    d_writer->writeTrailer();
    d_writer->writeHeader(true);
    d_writer.reset();
}

Tracker::BackgroundThread::BackgroundThread(
        std::shared_ptr<RecordWriter> record_writer,
        unsigned int memory_interval)
: d_writer(std::move(record_writer))
, d_memory_interval(memory_interval)
{
#ifdef __linux__
    d_procs_statm.open("/proc/self/status");
    if (!d_procs_statm) {
        throw IoError{"Failed to open /proc/self/status: " + std::string(strerror(errno))};
    }
#endif
}

uint64_t
Tracker::BackgroundThread::timeElapsed()
{
    std::chrono::milliseconds ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch());
    return ms.count();
}

size_t
Tracker::BackgroundThread::getRSS() const
{
#ifdef __linux__
    std::string proc_status{
            std::istreambuf_iterator<char>(d_procs_statm),
            std::istreambuf_iterator<char>()};
    d_procs_statm.clear();
    d_procs_statm.seekg(0);

    size_t rss_in_bytes;
    if (!getRSSFromProcStatus(proc_status, &rss_in_bytes)) {
        std::cerr << "WARNING: Failed to read RSS value from /proc/self/status" << std::endl;
        d_procs_statm.close();
        return 0;
    }

    return rss_in_bytes;
#elif defined(__APPLE__)
    struct mach_task_basic_info info;
    mach_msg_type_number_t infoCount = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, (task_info_t)&info, &infoCount)
        != KERN_SUCCESS)
        return (size_t)0L; /* Can't access? */
    return (size_t)info.resident_size;
#else
    return 0;
#endif
}

bool
Tracker::BackgroundThread::captureMemorySnapshot()
{
    auto now = timeElapsed();
    size_t rss = getRSS();
    if (rss == 0) {
        std::cerr << "Failed to detect RSS, deactivating tracking" << std::endl;
        Tracker::deactivate();
        return false;
    }

    std::lock_guard lock(*s_mutex);
    if (!d_writer->writeRecord(MemoryRecord{now, rss})) {
        if (Tracker::isActive()) {
            std::cerr << "Failed to write output, deactivating tracking" << std::endl;
            Tracker::deactivate();
        }
        return false;
    }

    return true;
}

void
Tracker::BackgroundThread::start()
{
    assert(d_thread.get_id() == std::thread::id());

    if (!captureMemorySnapshot()) {
        return;
    }

    d_thread = std::thread([&]() {
        RecursionGuard::setValue(true);
        while (true) {
            {
                std::unique_lock<std::mutex> lock(d_mutex);
                d_cv.wait_for(lock, d_memory_interval * 1ms, [this]() { return d_stop; });
                if (d_stop) {
                    return;
                }
            }

            if (!captureMemorySnapshot()) {
                return;
            }
        }
    });
}

void
Tracker::BackgroundThread::stop()
{
    {
        std::scoped_lock<std::mutex> lock(d_mutex);
        d_stop = true;
    }
    d_cv.notify_one();
    if (d_thread.joinable()) {
        try {
            d_thread.join();
        } catch (const std::system_error&) {
        }
    }
}

void
Tracker::prepareFork()
{
    // Don't do any custom track_allocation handling while inside fork
    RecursionGuard::setValue(true);
}

void
Tracker::parentFork()
{
    // We can continue tracking
    RecursionGuard::setValue(false);
}

void
Tracker::childFork()
{
    // Intentionally leak any old tracker. Its destructor cannot be called,
    // because it would try to destroy mutexes that might be locked by threads
    // that no longer exist, and to join a background thread that no longer
    // exists, and potentially to flush buffered output to a socket it no
    // longer owns. Note that d_instance_owner is always set after d_instance
    // and unset before d_instance.
    (void)s_instance_owner.release();

    // Likewise, leak our old mutex, and re-create it.
    (void)s_mutex.release();
    s_mutex.reset(new TrackerMutex);

    // Save a reference to the old tracker (if any), then unset our singleton.
    Tracker* old_tracker = s_instance;
    Tracker::deactivate();

    // If we inherited an active tracker, try to clone its record writer.
    std::unique_ptr<RecordWriter> new_writer;
    if (old_tracker && old_tracker->d_follow_fork) {
        new_writer = old_tracker->d_writer->cloneInChildProcess();
    }

    if (!new_writer) {
        // Either tracking wasn't active, or the tracker was using a sink that
        // can't be cloned. Leave our singleton unset and bail out. Note that
        // the old tracker's hooks may still be installed.  If this process
        // exits, trackDeallocation will be called to track the deallocation of
        // s_mutex when the process's globals are destroyed! To handle this,
        // the hooks must check the (static) isActive() flag before acquiring
        // s_mutex or calling any methods on the now null tracker singleton.
        RecursionGuard::setValue(false);
        return;
    }

    // Re-enable tracking with a brand new tracker.
    // Disable tracking until the new tracker is fully installed.
    s_instance_owner.reset(new Tracker(
            std::move(new_writer),
            old_tracker->d_unwind_native_frames,
            old_tracker->d_memory_interval,
            old_tracker->d_follow_fork,
            old_tracker->d_trace_python_allocators,
            old_tracker->d_reference_tracking));

    std::unique_lock lock(*s_mutex);
    PythonStackEmitter::s_tracker_generation++;
    tracking_api::Tracker::activate();
    RecursionGuard::setValue(false);
}

void
Tracker::recordMainThreadSkippedFrames(size_t num_frames)
{
    // The reader drops the outermost `num_frames` frames of every stack on
    // the main thread. Remember which frames those are, so that stacks that
    // don't start with them (because they come from a greenlet) can be
    // given them, and the reader drops those instead of real frames.
    assert(PyGILState_Check());
    d_main_tid = thread_id();

    std::vector<frame_walker::RawFrame> raw(num_frames + 1);
    size_t num_walked = frame_walker::walkCurrentThread(d_frame_layout, raw.data(), raw.size());
    assert(num_walked == num_frames + 1);
    num_walked = std::min(num_walked, raw.size());

    d_main_thread_skipped_frames.clear();
    for (size_t i = num_walked; i-- > num_walked - std::min(num_frames, num_walked);) {
        PyCodeObject* code = raw[i].code;
        code_object_id_t code_id = lookupCodeObject(code);
        if (!code_id) {
            code_id = registerCodeObject(code, frame_walker::readCodeObjectInfo(code));
        }
        d_main_thread_skipped_frames.push_back(
                Frame{code_id,
                      raw[i].instruction_offset,
                      !d_unwind_native_frames || raw[i].is_entry_frame});
    }
}

void
Tracker::addSkippedFramesIfNeeded(std::vector<Frame>& stack) const
{
    const auto& skipped = d_main_thread_skipped_frames;
    if (skipped.empty() || thread_id() != d_main_tid) {
        return;
    }

    // Only compare code objects: the skipped frames' callers may have moved
    // on if Tracker.__enter__ was called directly, and the reader still
    // drops those frames in that case.
    bool starts_with_skipped =
            stack.size() >= skipped.size()
            && std::equal(skipped.begin(), skipped.end(), stack.begin(), [](auto& a, auto& b) {
                   return a.code_object_id == b.code_object_id;
               });
    if (!starts_with_skipped) {
        stack.insert(stack.begin(), skipped.begin(), skipped.end());
    }
}

size_t
Tracker::computeMainTidSkip()
{
    // Determine how many frames from the current stack to elide from our
    // reported stack traces. This avoids showing the user frames above the one
    // that called `Tracker.__enter__`.
    assert(PyGILState_Check());

    PyFrameObject* frame = PyEval_GetFrame();

    size_t num_frames = 0;
    while (frame) {
        ++num_frames;
        frame = compat::frameGetBack(frame);
    }

    assert(num_frames > 0);
    return num_frames - 1;
}

bool
Tracker::areNativeTracesEnabled()
{
    return s_native_tracking_enabled;
}

void
Tracker::trackAllocationImpl(
        void* ptr,
        size_t size,
        hooks::Allocator func,
        const std::optional<NativeTrace>& trace)
{
    registerCachedThreadName();
    PythonStackEmitter::get().emitCurrentStack(*this, d_frame_layout);

    if (d_unwind_native_frames) {
        frame_id_t native_index = 0;

        // Skip the internal frames so we don't need to filter them later.
        if (trace && trace.value().size()) {
            native_index =
                    d_native_trace_tree.getTraceIndex(trace.value(), [&](frame_id_t ip, uint32_t index) {
                        return d_writer->writeRecord(UnresolvedNativeFrame{ip, index});
                    });
        }
        AllocationRecord record{reinterpret_cast<uintptr_t>(ptr), size, func, native_index};
        if (!d_writer->writeThreadSpecificRecord(thread_id(), record)) {
            std::cerr << "Failed to write output, deactivating tracking" << std::endl;
            deactivate();
        }
    } else {
        AllocationRecord record{reinterpret_cast<uintptr_t>(ptr), size, func};
        if (!d_writer->writeThreadSpecificRecord(thread_id(), record)) {
            std::cerr << "Failed to write output, deactivating tracking" << std::endl;
            deactivate();
        }
    }
}

void
Tracker::trackDeallocationImpl(void* ptr, size_t size, hooks::Allocator func)
{
    registerCachedThreadName();
    AllocationRecord record{reinterpret_cast<uintptr_t>(ptr), size, func};
    if (!d_writer->writeThreadSpecificRecord(thread_id(), record)) {
        std::cerr << "Failed to write output, deactivating tracking" << std::endl;
        deactivate();
    }
}

void
Tracker::trackObjectImpl(PyObject* obj, int event, const std::optional<NativeTrace>& trace)
{
    registerCachedThreadName();
    PythonStackEmitter::get().emitCurrentStack(*this, d_frame_layout);

    if (event == 0) {  // Creation event
        d_tracked_objects.emplace(obj);

        if (d_unwind_native_frames) {
            frame_id_t native_index = 0;
            // Skip the internal frames so we don't need to filter them later.
            if (trace && trace.value().size()) {
                native_index = d_native_trace_tree.getTraceIndex(
                        trace.value(),
                        [&](frame_id_t ip, uint32_t index) {
                            return d_writer->writeRecord(UnresolvedNativeFrame{ip, index});
                        });
            }

            ObjectRecord record{reinterpret_cast<uintptr_t>(obj), true, native_index};
            if (!d_writer->writeThreadSpecificRecord(thread_id(), record)) {
                std::cerr << "Failed to write output, deactivating tracking" << std::endl;
                deactivate();
            }
        } else {
            ObjectRecord record{reinterpret_cast<uintptr_t>(obj), true};
            if (!d_writer->writeThreadSpecificRecord(thread_id(), record)) {
                std::cerr << "Failed to write output, deactivating tracking" << std::endl;
                deactivate();
            }
        }
    } else {  // Destruction event
        d_tracked_objects.erase(obj);
        ObjectRecord record{reinterpret_cast<uintptr_t>(obj), false};
        if (!d_writer->writeThreadSpecificRecord(thread_id(), record)) {
            std::cerr << "Failed to write output, deactivating tracking" << std::endl;
            deactivate();
        }
    }
}

void
Tracker::invalidate_module_cache_impl()
{
    // libarrow can be loaded after tracking starts.
    hooks::ensureArrowHooksAreValid();
    d_patcher.overwrite_symbols();
    updateModuleCacheImpl();
}

#ifdef __linux__
static int
dl_iterate_phdr_callback(struct dl_phdr_info* info, [[maybe_unused]] size_t size, void* data)
{
    auto& mappings = *reinterpret_cast<std::vector<ImageSegments>*>(data);
    const char* filename = info->dlpi_name;
    std::string executable;
    assert(filename != nullptr);
    if (!filename[0]) {
        executable = get_executable();
        filename = executable.c_str();
    }
    if (::starts_with(filename, "linux-vdso.so")) {
        // This cannot be resolved to anything, so don't write it to the file
        return 0;
    }

    std::vector<Segment> segments;
    for (int i = 0; i < info->dlpi_phnum; i++) {
        const auto& phdr = info->dlpi_phdr[i];
        if (phdr.p_type == PT_LOAD) {
            segments.emplace_back(Segment{phdr.p_vaddr, phdr.p_memsz});
        }
    }

    mappings.push_back({filename, info->dlpi_addr, std::move(segments)});
    return 0;
}
#endif

void
Tracker::updateModuleCacheImpl()
{
    if (!d_unwind_native_frames) {
        return;
    }

    static size_t s_last_mappings_size = 20;

    std::vector<ImageSegments> mappings;
    mappings.reserve(s_last_mappings_size + 1);

#ifdef __linux__
    dl_iterate_phdr(&dl_iterate_phdr_callback, &mappings);
#elif defined(__APPLE__)
    uint32_t c = _dyld_image_count();
    for (uint32_t i = 0; i < c; i++) {
        const struct mach_header* header = _dyld_get_image_header(i);
        auto slide = static_cast<uintptr_t>(_dyld_get_image_vmaddr_slide(i));
        const char* image_name = _dyld_get_image_name(i);
        std::vector<Segment> segments;

        const segment_command_t* current_segment_cmd;
        uintptr_t current_cmd = reinterpret_cast<uintptr_t>(header) + sizeof(mach_header_t);
        for (uint j = 0; j < header->ncmds; j++, current_cmd += current_segment_cmd->cmdsize) {
            current_segment_cmd = reinterpret_cast<const segment_command_t*>(current_cmd);
            if (current_segment_cmd->cmd == ARCH_LC_SEGMENT) {
                segments.emplace_back(Segment{current_segment_cmd->vmaddr, current_segment_cmd->vmsize});
            }
        }

        mappings.push_back({image_name, slide, std::move(segments)});
    }
#endif

    s_last_mappings_size = mappings.size();

    if (!d_writer->writeMappings(mappings)) {
        std::cerr << "memray: Failed to write output, deactivating tracking" << std::endl;
        Tracker::deactivate();
        return;
    }
}

void
Tracker::registerThreadNameImpl(const char* name)
{
    RecursionGuard guard;
    dropCachedThreadName();
    if (!d_writer->writeThreadSpecificRecord(thread_id(), ThreadRecord{name})) {
        std::cerr << "memray: Failed to write output, deactivating tracking" << std::endl;
        deactivate();
    }
}

void
Tracker::registerCachedThreadName()
{
    if (d_cached_thread_names.empty()) {
        return;
    }

    auto it = d_cached_thread_names.find((uint64_t)(pthread_self()));
    if (it != d_cached_thread_names.end()) {
        auto& name = it->second;
        if (!d_writer->writeThreadSpecificRecord(thread_id(), ThreadRecord{name.c_str()})) {
            std::cerr << "memray: Failed to write output, deactivating tracking" << std::endl;
            deactivate();
        }
        d_cached_thread_names.erase(it);
    }
}

void
Tracker::dropCachedThreadName()
{
    d_cached_thread_names.erase((uint64_t)(pthread_self()));
}

void
Tracker::registerReferenceTrackingHooks() const noexcept
{
    compat::refTracerSetTracer(intercept::pyreftracer, nullptr);
}

void
Tracker::unregisterReferenceTrackingHooks() const noexcept
{
    compat::refTracerSetTracer(nullptr, nullptr);
}

std::unordered_set<PyObject*>
Tracker::getSurvivingObjects()
{
    std::scoped_lock lock(*s_mutex);
    RecursionGuard guard;

    std::unordered_set<PyObject*> surviving_objects;
    // remove everything with 0 refcount
    for (auto obj : d_tracked_objects) {
#ifndef Py_GIL_DISABLED
        // CPython used to have some bugs where deallocation of objects
        // wasn't triggering the tracking hooks and that was causing us
        // to see deleted objects at this stage. This check is left here
        // as a precaution to know if CPython is still missing some cases
        // still. The check is not done in free-threaded builds because
        // the semantics of Py_REFCNT are more complicated and this may not
        // do what we want.
        if (Py_REFCNT(obj) == 0) {
            Py_UNREACHABLE();
        }
#endif
        Py_INCREF(obj);
        surviving_objects.insert(obj);
    }
    d_tracked_objects.clear();

    // While we hold s_mutex and our reference tracking hooks are installed,
    // other threads can't destroy any objects. As soon as we uninstall the
    // tracking hooks, objects can be destroyed by background threads without
    // us finding out. This means we can't uninstall the hooks until after
    // we've incremented the reference count of all the surviving objects.
    if (d_reference_tracking) {
        unregisterReferenceTrackingHooks();
    }
    return surviving_objects;
}

code_object_id_t
Tracker::lookupCodeObject(PyCodeObject* code_ptr) const
{
    auto it = d_code_object_cache.find(code_ptr);
    return it == d_code_object_cache.end() ? 0 : it->second;
}

code_object_id_t
Tracker::registerCodeObject(PyCodeObject* code_ptr, CodeObjectInfo code_info)
{
    assert(!lookupCodeObject(code_ptr));
    code_object_id_t code_id = d_next_code_object_id++;
    d_code_object_cache[code_ptr] = code_id;

    // Write the code object record
    pycode_map_val_t code_record{code_id, std::move(code_info)};

    if (!d_writer->writeRecord(code_record)) {
        std::cerr << "memray: Failed to write code object record, deactivating tracking" << std::endl;
        deactivate();
    }

    return code_id;
}

void
Tracker::forgetCodeObject(PyCodeObject* code)
{
    d_code_object_cache.erase(code);
}

bool
Tracker::popFrames(uint32_t count)
{
    const FramePop entry{count};
    if (!d_writer->writeThreadSpecificRecord(thread_id(), entry)) {
        std::cerr << "memray: Failed to write output, deactivating tracking" << std::endl;
        deactivate();
        return false;
    }
    return true;
}

bool
Tracker::pushFrame(const Frame& cooked)
{
    const FramePush entry{cooked};
    if (!d_writer->writeThreadSpecificRecord(thread_id(), entry)) {
        std::cerr << "memray: Failed to write output, deactivating tracking" << std::endl;
        deactivate();
        return false;
    }
    return true;
}

void
Tracker::activate()
{
    // NOTE: Tracker::s_mutex must be held
    s_instance = s_instance_owner.get();
}

void
Tracker::deactivate()
{
    s_instance = nullptr;
}

bool
Tracker::isActive()
{
    return s_instance != nullptr;
}

// Static methods managing the singleton

PyObject*
Tracker::createTracker(
        std::unique_ptr<RecordWriter> record_writer,
        bool native_traces,
        unsigned int memory_interval,
        bool follow_fork,
        bool trace_python_allocators,
        bool reference_tracking)
{
    s_instance_owner.reset(new Tracker(
            std::move(record_writer),
            native_traces,
            memory_interval,
            follow_fork,
            trace_python_allocators,
            reference_tracking));

    std::unique_lock lock(*s_mutex);
    PythonStackEmitter::s_tracker_generation++;
    tracking_api::Tracker::activate();
    Py_RETURN_NONE;
}

PyObject*
Tracker::destroyTracker()
{
    // Note: the GIL is used for synchronization of the singleton
    s_instance_owner.reset();
    Py_RETURN_NONE;
}

Tracker*
Tracker::getTracker()
{
    return s_instance;
}

static struct
{
    PyMemAllocatorEx raw;
    PyMemAllocatorEx mem;
    PyMemAllocatorEx obj;
} s_orig_pymalloc_allocators;

void
Tracker::registerPymallocHooks() const noexcept
{
    assert(d_trace_python_allocators);
    PyMemAllocatorEx alloc;

    PyMem_GetAllocator(PYMEM_DOMAIN_RAW, &alloc);
    if (alloc.free == &intercept::pymalloc_free) {
        // Nothing to do; our hooks are already installed.
        return;
    }

    alloc.malloc = intercept::pymalloc_malloc;
    alloc.calloc = intercept::pymalloc_calloc;
    alloc.realloc = intercept::pymalloc_realloc;
    alloc.free = intercept::pymalloc_free;
    PyMem_GetAllocator(PYMEM_DOMAIN_RAW, &s_orig_pymalloc_allocators.raw);
    PyMem_GetAllocator(PYMEM_DOMAIN_MEM, &s_orig_pymalloc_allocators.mem);
    PyMem_GetAllocator(PYMEM_DOMAIN_OBJ, &s_orig_pymalloc_allocators.obj);
    alloc.ctx = &s_orig_pymalloc_allocators.raw;
    PyMem_SetAllocator(PYMEM_DOMAIN_RAW, &alloc);
    alloc.ctx = &s_orig_pymalloc_allocators.mem;
    PyMem_SetAllocator(PYMEM_DOMAIN_MEM, &alloc);
    alloc.ctx = &s_orig_pymalloc_allocators.obj;
    PyMem_SetAllocator(PYMEM_DOMAIN_OBJ, &alloc);
}

void
Tracker::unregisterPymallocHooks() const noexcept
{
    assert(d_trace_python_allocators);
    PyMem_SetAllocator(PYMEM_DOMAIN_RAW, &s_orig_pymalloc_allocators.raw);
    PyMem_SetAllocator(PYMEM_DOMAIN_MEM, &s_orig_pymalloc_allocators.mem);
    PyMem_SetAllocator(PYMEM_DOMAIN_OBJ, &s_orig_pymalloc_allocators.obj);
}

void
set_up_pthread_fork_handlers()
{
    static std::once_flag once;
    call_once(once, [] {
        // On macOS the recursion guard uses pthread TLS keys, so we
        // need to create those keys before we install our handlers.
        RecursionGuard::initialize();
        pthread_atfork(&Tracker::prepareFork, &Tracker::parentFork, NULL);
    });
}

}  // namespace memray::tracking_api
