#pragma once

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <fstream>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <pthread.h>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_set>

#include <unwind.h>

#include "frameobject.h"

#if defined(__linux__)
#    define UNW_LOCAL_ONLY
#    include <libunwind.h>
#elif defined(__APPLE__)
#    include <execinfo.h>
#endif

#include "frame_tree.h"
#include "frame_walker.h"
#include "hooks.h"
#include "linker_shenanigans.h"
#include "record_writer.h"
#include "records.h"

#if defined(USE_MEMRAY_TLS_MODEL)
#    if defined(__GLIBC__)
#        define MEMRAY_FAST_TLS __attribute__((tls_model("initial-exec")))
#    else
#        define MEMRAY_FAST_TLS __attribute__((tls_model("local-dynamic")))
#    endif
#else
#    define MEMRAY_FAST_TLS
#endif

namespace memray::tracking_api {

class TrackerMutex
{
  public:
    TrackerMutex() = default;

#if defined(__linux__) && defined(__GLIBC__)
    ~TrackerMutex()
    {
        pthread_mutex_destroy(&d_mutex);
    }
#endif

    TrackerMutex(const TrackerMutex&) = delete;
    TrackerMutex& operator=(const TrackerMutex&) = delete;

    void lock()
    {
#if defined(__linux__) && defined(__GLIBC__)
        int error = pthread_mutex_lock(&d_mutex);
        if (error != 0) {
            throw std::system_error(error, std::generic_category(), "pthread_mutex_lock");
        }
#else
        d_mutex.lock();
#endif
        d_owner.store(std::this_thread::get_id(), std::memory_order_relaxed);
    }

    void unlock() noexcept
    {
        d_owner.store(std::thread::id(), std::memory_order_relaxed);
#if defined(__linux__) && defined(__GLIBC__)
        pthread_mutex_unlock(&d_mutex);
#else
        d_mutex.unlock();
#endif
    }

    // Only the owning thread ever stores its own ID, so this can't return
    // true for a thread that doesn't hold the lock, or vice versa.
    bool heldByCurrentThread() const noexcept
    {
        return d_owner.load(std::memory_order_relaxed) == std::this_thread::get_id();
    }

  private:
#if defined(__linux__) && defined(__GLIBC__)
    pthread_mutex_t d_mutex = PTHREAD_ADAPTIVE_MUTEX_INITIALIZER_NP;
#else
    std::mutex d_mutex;
#endif
    std::atomic<std::thread::id> d_owner{};
};

bool
getRSSFromProcStatus(const std::string& proc_status, size_t* rss_in_bytes);

struct RecursionGuard
{
    RecursionGuard()
    : wasLocked(isActive())
    {
        setValue(true);
    }

    ~RecursionGuard()
    {
        setValue(wasLocked);
    }

    const bool wasLocked;

#ifdef __linux__
    MEMRAY_FAST_TLS static thread_local bool _isActive;

    static void initialize()
    {
    }

    static __attribute__((always_inline)) inline bool isActive()
    {
        return _isActive;
    }

    static __attribute__((always_inline)) inline void setValue(bool value)
    {
        _isActive = value;
    }
#else
    static pthread_key_t isActiveKey;

    static void initialize()
    {
        if (0 != pthread_key_create(&isActiveKey, NULL)) {
            throw std::runtime_error{"Failed to create pthread key"};
        }
    }

    static __attribute__((always_inline)) inline bool isActive()
    {
        return pthread_getspecific(isActiveKey) != 0;
    }

    static __attribute__((always_inline)) inline void setValue(bool value)
    {
        static bool true_constant = true;
        if (pthread_setspecific(isActiveKey, value ? &true_constant : (void*)0) != 0) {
            abort();
        }
        return;
    }
#endif
};

/**
 * Install our pthread fork handlers.
 */
void
set_up_pthread_fork_handlers();

class NativeTrace
{
  public:
    using ip_t = frame_id_t;

    NativeTrace(std::vector<ip_t>& data)
    : d_data(data)
    {
    }

    auto begin() const
    {
        return std::reverse_iterator(d_data.begin() + d_skip + d_size);
    }
    auto end() const
    {
        return std::reverse_iterator(d_data.begin() + d_skip);
    }
    ip_t operator[](size_t i) const
    {
        return d_data[d_skip + d_size - 1 - i];
    }
    int size() const
    {
        return d_size;
    }
    __attribute__((always_inline)) inline bool fill(size_t skip)
    {
        size_t size;
        while (true) {
#ifdef __linux__
            size = unw_backtrace((void**)d_data.data(), d_data.size());
#elif defined(__APPLE__)
            size = ::backtrace((void**)d_data.data(), d_data.size());
#else
            return 0;
#endif
            if (size < d_data.size()) {
                break;
            }

            d_data.resize(d_data.size() * 2);
        }
        d_size = size > skip ? size - skip : 0;
        d_skip = skip;
        return d_size > 0;
    }

    static void setup()
    {
#ifdef __linux__
        // configure libunwind for better speed
        if (unw_set_caching_policy(unw_local_addr_space, UNW_CACHE_PER_THREAD)) {
            fprintf(stderr, "WARNING: Failed to enable per-thread libunwind caching.\n");
        }
#    if (UNW_VERSION_MAJOR > 1 && UNW_VERSION_MINOR >= 3)
        if (unw_set_cache_size(unw_local_addr_space, 1024, 0)) {
            fprintf(stderr, "WARNING: Failed to set libunwind cache size.\n");
        }
#    endif
#else
        return;
#endif
    }

    static inline void flushCache()
    {
#ifdef __linux__
        unw_flush_cache(unw_local_addr_space, 0, 0);
#else
        return;
#endif
    }

  private:
    size_t d_size = 0;
    size_t d_skip = 0;
    std::vector<ip_t>& d_data;
};

/**
 * Singleton managing all the global state and functionality of the tracing mechanism
 *
 * This class acts as the only interface to the tracing functionality and encapsulates all the
 * required global state. *All access* must be done through the singleton interface as the singleton
 * has the same lifetime of the entire program. The singleton can be activated and deactivated to
 * temporarily stop the tracking as desired. At each allocation, the allocating thread reads its own
 * Python stack directly, without needing the GIL, and records how it changed since the last time.
 * */
class Tracker
{
  public:
    // Constructors
    ~Tracker();

    Tracker(Tracker& other) = delete;
    Tracker(Tracker&& other) = delete;
    void operator=(const Tracker&) = delete;
    void operator=(Tracker&&) = delete;

    // Interface to get the tracker instance
    static PyObject* createTracker(
            std::unique_ptr<RecordWriter> record_writer,
            bool native_traces,
            unsigned int memory_interval,
            bool follow_fork,
            bool trace_python_allocators,
            bool reference_tracking);
    static PyObject* destroyTracker();
    static Tracker* getTracker();

    // Allocation tracking interface
    __attribute__((always_inline)) inline static void
    trackAllocation(void* ptr, size_t size, hooks::Allocator func)
    {
        if (RecursionGuard::isActive() || !Tracker::isActive()) {
            return;
        }
        RecursionGuard guard;

        std::optional<NativeTrace> trace{std::nullopt};
        if (Tracker::areNativeTracesEnabled()) {
            if (!prepareNativeTrace(trace)) {
                return;
            }
            // Skip the internal frames so we don't need to filter them later.
            trace.value().fill(1);
        }

        std::unique_lock lock(*s_mutex);
        Tracker* tracker = getTracker();
        if (tracker) {
            tracker->trackAllocationImpl(ptr, size, func, trace);
        }
    }

    // Object tracking interface
    __attribute__((always_inline)) inline static void trackObject(PyObject* obj, int event)
    {
        if (RecursionGuard::isActive() || !Tracker::isActive()) {
            return;
        }
        RecursionGuard guard;

        std::optional<NativeTrace> trace{std::nullopt};
        // Only creation events store native stacks.
        if (event == 0 && Tracker::areNativeTracesEnabled()) {
            if (!prepareNativeTrace(trace)) {
                return;
            }
            // Skip the internal frames so we don't need to filter them later.
            trace.value().fill(1);
        }

        std::unique_lock lock(*s_mutex);
        Tracker* tracker = getTracker();
        if (tracker) {
            tracker->trackObjectImpl(obj, event, trace);
        }
    }

    static inline bool prepareNativeTrace(std::optional<NativeTrace>& trace)
    {
        auto t_trace_data_ptr = static_cast<std::vector<NativeTrace::ip_t>*>(
                pthread_getspecific(s_native_unwind_vector_key));
        if (!t_trace_data_ptr) {
            t_trace_data_ptr = new std::vector<NativeTrace::ip_t>();
            if (pthread_setspecific(s_native_unwind_vector_key, t_trace_data_ptr) != 0) {
                Tracker::deactivate();
                std::cerr << "memray: pthread_setspecific failed" << std::endl;
                delete t_trace_data_ptr;
                return false;
            }
            t_trace_data_ptr->resize(128);
        }
        trace.emplace(*t_trace_data_ptr);
        return true;
    }

    __attribute__((always_inline)) inline static void
    trackDeallocation(void* ptr, size_t size, hooks::Allocator func)
    {
        if (RecursionGuard::isActive() || !Tracker::isActive()) {
            return;
        }
        RecursionGuard guard;

        std::unique_lock lock(*s_mutex);
        Tracker* tracker = getTracker();
        if (tracker) {
            tracker->trackDeallocationImpl(ptr, size, func);
        }
    }

    __attribute__((always_inline)) inline static void invalidate_module_cache()
    {
        if (RecursionGuard::isActive() || !Tracker::isActive()) {
            return;
        }
        RecursionGuard guard;

        std::unique_lock lock(*s_mutex);
        Tracker* tracker = getTracker();
        if (tracker) {
            tracker->invalidate_module_cache_impl();
        }
    }

    __attribute__((always_inline)) inline static void registerThreadName(const char* name)
    {
        if (RecursionGuard::isActive() || !Tracker::isActive()) {
            return;
        }
        RecursionGuard guard;

        std::unique_lock lock(*s_mutex);
        Tracker* tracker = getTracker();
        if (tracker) {
            tracker->registerThreadNameImpl(name);
        }
    }

    inline static void registerThreadNameById(uint64_t thread, const char* name)
    {
        if (RecursionGuard::isActive() || !Tracker::isActive()) {
            return;
        }
        RecursionGuard guard;

        std::unique_lock lock(*s_mutex);
        Tracker* tracker = getTracker();
        if (tracker) {
            if (thread == (uint64_t)(pthread_self())) {
                tracker->registerThreadNameImpl(name);
            } else {
                // We've got a different thread's name, but don't know what id
                // has been assigned to that thread (if any!). Set this update
                // aside to be handled later, from that thread.
                tracker->d_cached_thread_names.emplace(thread, name);
            }
        }
    }

    // stack interface
    bool pushFrame(const Frame& frame);
    bool popFrames(uint32_t count);

    // On the main thread, prepend the frames the reader drops from the
    // start of every stack, if `stack` (outermost first) doesn't have them.
    void addSkippedFramesIfNeeded(std::vector<Frame>& stack) const;

    // code object registration
    // Returns the code object's ID, or 0 if it hasn't been registered.
    code_object_id_t lookupCodeObject(PyCodeObject* code_ptr) const;
    // Registers a code object that isn't registered yet, returning its ID.
    code_object_id_t registerCodeObject(PyCodeObject* code_ptr, CodeObjectInfo code_info);
    void forgetCodeObject(PyCodeObject* code);
    // Called when any code object is destroyed.
    static void forgetDestroyedCodeObject(PyCodeObject* code);

    // Interface to activate/deactivate the tracking
    static bool isActive();
    static void activate();
    static void deactivate();

    static void prepareFork();
    static void parentFork();
    static void childFork();
    std::unordered_set<PyObject*> getSurvivingObjects();

  private:
    class BackgroundThread
    {
      public:
        // Constructors
        BackgroundThread(std::shared_ptr<RecordWriter> record_writer, unsigned int memory_interval);

        // Methods
        void start();
        void stop();

      private:
        // Data members
        std::shared_ptr<RecordWriter> d_writer;
        bool d_stop{false};
        unsigned int d_memory_interval;
        std::mutex d_mutex;
        std::condition_variable d_cv;
        std::thread d_thread;
        mutable std::ifstream d_procs_statm;

        // Methods
        size_t getRSS() const;
        static uint64_t timeElapsed();
        bool captureMemorySnapshot();
    };

    // Data members
    static std::unique_ptr<TrackerMutex> s_mutex;
    static pthread_key_t s_native_unwind_vector_key;
    static std::unique_ptr<Tracker> s_instance_owner;
    static std::atomic<Tracker*> s_instance;

    const frame_walker::FrameLayout d_frame_layout;
    std::shared_ptr<RecordWriter> d_writer;
    FrameTree d_native_trace_tree;
    const bool d_unwind_native_frames;
    const unsigned int d_memory_interval;
    const bool d_follow_fork;
    const bool d_trace_python_allocators;
    const bool d_reference_tracking;
    linker::SymbolPatcher d_patcher;
    std::unique_ptr<BackgroundThread> d_background_thread;

    thread_id_t d_main_tid{};
    std::vector<Frame> d_main_thread_skipped_frames;
    std::unordered_map<PyCodeObject*, code_object_id_t> d_code_object_cache;
    code_object_id_t d_next_code_object_id{1};
    std::unordered_map<uint64_t, std::string> d_cached_thread_names;
    std::unordered_set<PyObject*> d_tracked_objects;

    // Methods
    static size_t computeMainTidSkip();
    void recordMainThreadSkippedFrames(size_t num_frames);

    void trackAllocationImpl(
            void* ptr,
            size_t size,
            hooks::Allocator func,
            const std::optional<NativeTrace>& trace);
    void trackDeallocationImpl(void* ptr, size_t size, hooks::Allocator func);
    void trackObjectImpl(PyObject* obj, int event, const std::optional<NativeTrace>& trace);
    void invalidate_module_cache_impl();
    void updateModuleCacheImpl();
    void registerThreadNameImpl(const char* name);
    void registerCachedThreadName();
    void dropCachedThreadName();
    void registerPymallocHooks() const noexcept;
    void unregisterPymallocHooks() const noexcept;
    void registerReferenceTrackingHooks() const noexcept;
    void unregisterReferenceTrackingHooks() const noexcept;

    explicit Tracker(
            std::unique_ptr<RecordWriter> record_writer,
            bool native_traces,
            unsigned int memory_interval,
            bool follow_fork,
            bool trace_python_allocators,
            bool reference_tracking);

    static bool areNativeTracesEnabled();
};

}  // namespace memray::tracking_api
