"""Utilities used only by memray's test suite.

If you make changes to this file that move functions around, you will need to
change line numbers in test files as well.
"""
import sys

from posix.mman cimport MAP_ANONYMOUS
from posix.mman cimport MAP_FAILED
from posix.mman cimport MAP_SHARED
from posix.mman cimport PROT_WRITE
from posix.mman cimport mmap
from posix.mman cimport munmap
from posix.unistd cimport read
from posix.unistd cimport write

from _memray.alloc cimport PyMem_Calloc
from _memray.alloc cimport PyMem_Free
from _memray.alloc cimport PyMem_Malloc
from _memray.alloc cimport PyMem_RawCalloc
from _memray.alloc cimport PyMem_RawFree
from _memray.alloc cimport PyMem_RawMalloc
from _memray.alloc cimport PyMem_RawRealloc
from _memray.alloc cimport PyMem_Realloc
from _memray.alloc cimport PyObject_Calloc
from _memray.alloc cimport PyObject_Free
from _memray.alloc cimport PyObject_Malloc
from _memray.alloc cimport PyObject_Realloc
from _memray.alloc cimport aligned_alloc
from _memray.alloc cimport calloc
from _memray.alloc cimport free
from _memray.alloc cimport malloc
from _memray.alloc cimport memalign
from _memray.alloc cimport posix_memalign
from _memray.alloc cimport pvalloc
from _memray.alloc cimport realloc
from _memray.alloc cimport valloc
from _memray.pthread cimport pthread_create
from _memray.pthread cimport pthread_join
from _memray.pthread cimport pthread_t
from cpython.pylifecycle cimport Py_FinalizeEx
from libc.errno cimport errno
from libc.stdint cimport uintptr_t
from libc.stdlib cimport exit as _exit
from libcpp.string cimport string
from libcpp.vector cimport vector

from ._destination import Destination


cdef extern from *:
    """
    #ifdef __linux__
    # include <sys/prctl.h>
    inline int set_thread_name_impl(const char* new_name)
    {
        return prctl(PR_SET_NAME, new_name, NULL, NULL, NULL);
    }
    #else
    # include <errno.h>
    inline int set_thread_name_impl(const char* new_name)
    {
        errno = ENOTSUP;
        return -1;
    }
    #endif
    """
    int set_thread_name_impl(const char* new_name)


cdef extern from "hooks.h":
    void free_sized(void*, size_t)
    void free_aligned_sized(void*, size_t, size_t)


def set_thread_name(new_name):
    return set_thread_name_impl(new_name)


cdef class MemoryAllocator:
    cdef void* ptr

    def __cinit__(self):
        self.ptr = NULL

    def free(self):
        if self.ptr == NULL:
            raise RuntimeError("Pointer cannot be NULL")
        free(self.ptr)
        self.ptr = NULL

    def free_sized(self, size_t size):
        if self.ptr == NULL:
            raise RuntimeError("Pointer cannot be NULL")
        if &free_sized == NULL:
            return False
        free_sized(self.ptr, size)
        self.ptr = NULL
        return True

    def free_aligned_sized(self, size_t size):
        if self.ptr == NULL:
            raise RuntimeError("Pointer cannot be NULL")
        if &free_aligned_sized == NULL:
            return False
        free_aligned_sized(self.ptr, sizeof(void*), size)
        self.ptr = NULL
        return True

    def malloc(self, size_t size):
        self.ptr = malloc(size)
        return self.ptr != NULL

    def calloc(self, size_t size):
        self.ptr = calloc(1, size)
        return self.ptr != NULL

    def realloc(self, size_t size):
        self.ptr = malloc(1)
        self.ptr = realloc(self.ptr, size)
        return self.ptr != NULL

    def posix_memalign(self, size_t size):
        rc = posix_memalign(&self.ptr, sizeof(void*), size)
        return rc == 0 and self.ptr != NULL

    def aligned_alloc(self, size_t size):
        self.ptr = aligned_alloc(sizeof(void*), size)
        return self.ptr != NULL

    def memalign(self, size_t size):
        self.ptr = memalign(sizeof(void*), size)
        return self.ptr != NULL

    def valloc(self, size_t size):
        self.ptr = valloc(size)
        return self.ptr != NULL

    def pvalloc(self, size_t size):
        self.ptr = pvalloc(size)
        return self.ptr != NULL

    def run_in_pthread(self, callback):
        cdef pthread_t thread
        cdef int ret = pthread_create(&thread, NULL, &_pthread_worker, <void*>callback)
        if ret != 0:
            raise RuntimeError("Failed to create thread")
        with nogil:
            pthread_join(thread, NULL)


cpdef enum class PymallocDomain:
    PYMALLOC_RAW = 1
    PYMALLOC_MEM = 2
    PYMALLOC_OBJECT = 3


cdef class PymallocMemoryAllocator:
    cdef void* ptr
    cdef PymallocDomain domain

    def __cinit__(self, PymallocDomain domain):
        self.ptr = NULL
        self.domain = domain

    def free(self):
        if self.ptr == NULL:
            raise RuntimeError("Pointer cannot be NULL")
        if self.domain == PymallocDomain.PYMALLOC_RAW:
            PyMem_RawFree(self.ptr)
        elif self.domain == PymallocDomain.PYMALLOC_MEM:
            PyMem_Free(self.ptr)
        elif self.domain == PymallocDomain.PYMALLOC_OBJECT:
            PyObject_Free(self.ptr)
        else:
            raise RuntimeError("Invlid pymalloc domain")
        self.ptr = NULL

    def malloc(self, size_t size):
        if self.domain == PymallocDomain.PYMALLOC_RAW:
            self.ptr = PyMem_RawMalloc(size)
        elif self.domain == PymallocDomain.PYMALLOC_MEM:
            self.ptr = PyMem_Malloc(size)
        elif self.domain == PymallocDomain.PYMALLOC_OBJECT:
            self.ptr = PyObject_Malloc(size)
        else:
            raise RuntimeError("Invlid pymalloc domain")

        return self.ptr != NULL

    def calloc(self, size_t size):
        if self.domain == PymallocDomain.PYMALLOC_RAW:
            self.ptr = PyMem_RawCalloc(1, size)
        elif self.domain == PymallocDomain.PYMALLOC_MEM:
            self.ptr = PyMem_Calloc(1, size)
        elif self.domain == PymallocDomain.PYMALLOC_OBJECT:
            self.ptr = PyObject_Calloc(1, size)
        else:
            raise RuntimeError("Invlid pymalloc domain")

        return self.ptr != NULL

    def realloc(self, size_t size):
        if self.domain == PymallocDomain.PYMALLOC_RAW:
            self.ptr = PyMem_RawRealloc(self.ptr, size)
        elif self.domain == PymallocDomain.PYMALLOC_MEM:
            self.ptr = PyMem_Realloc(self.ptr, size)
        elif self.domain == PymallocDomain.PYMALLOC_OBJECT:
            self.ptr = PyObject_Realloc(self.ptr, size)
        else:
            raise RuntimeError("Invlid pymalloc domain")

        return self.ptr != NULL

cdef do_not_optimize_ptr(void* ptr):
    return ptr == <void*>(1)

def _cython_nested_allocation(allocator_fn, size):
    allocator_fn(size)
    cdef void* p = valloc(size);
    do_not_optimize_ptr(p)
    free(p)

cdef class MmapAllocator:
    cdef uintptr_t _address

    def __cinit__(self, size, address=0):
        cdef uintptr_t start_address = address

        self._address = <uintptr_t>mmap(<void *>start_address, size, PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0)
        if <void *>self._address == MAP_FAILED:
            raise MemoryError

    @property
    def address(self):
        return self._address

    def munmap(self, length, offset=0):
        cdef uintptr_t addr = self._address + <uintptr_t> offset
        cdef int ret = munmap(<void *>addr, length)
        if ret != 0:
            raise MemoryError(f"munmap rcode: {ret} errno: {errno}")

cdef void* _pthread_worker(void* arg) noexcept with gil:
    (<object> arg)()

def _cython_allocate_in_two_places(size_t size):
    cdef void* a = allocation_place_a(size)
    do_not_optimize_ptr(a)
    cdef void* b = allocation_place_b(size)
    do_not_optimize_ptr(b)
    free(a)
    free(b)

cdef void* allocation_place_a(size_t size):
    return valloc(size)

cdef void* allocation_place_b(size_t size):
    return valloc(size)

def function_caller(func):
    func()

def allocate_without_gil_held(int wake_up_main_fd, int wake_up_thread_fd):
    cdef char buf = 0
    cdef int write_rc = 0
    cdef int read_rc = 0
    cdef void* p = NULL
    with nogil:
        while write_rc != 1:
            write_rc = write(wake_up_main_fd, &buf, 1)
        while read_rc != 1:
            read_rc = read(wake_up_thread_fd, &buf, 1)
        p = valloc(1234)
    do_not_optimize_ptr(p)
    free(p)
    p = valloc(4321)
    do_not_optimize_ptr(p)
    free(p)

def allocate_cpp_vector(size_t size):
    cdef vector[int] v;
    cdef size_t nelems = <size_t>(size / sizeof(int))
    v.reserve(nelems)
    return v.size()


def fill_cpp_vector(size_t size):
    cdef vector[int] v
    cdef size_t nelems = <size_t>(size / sizeof(int))
    for i in range(nelems):
        v.push_back(i)
    return v.size()

def exit(bint py_finalize=False):
    if py_finalize:
        Py_FinalizeEx()
        _exit(0)
    else:
        with nogil:
            _exit(0)


cdef class PrimeCaches:
    cdef object old_profile
    def __enter__(self):
        self.old_profile = sys.getprofile()
        sys.setprofile(lambda *args: None)
        return self
    def __exit__(self, *args):
        sys.setprofile(self.old_profile)


cdef extern from *:
    """
    #include <cstddef>
    #include <cstring>
    #include <stdexcept>
    #include <vector>
    #include "frame_walker.h"
    #include "frameobject.h"

    namespace {
    PyObject* walk_current_thread_impl()
    {
        using namespace memray::frame_walker;
        FrameLayout layout = loadFrameLayout();
        std::vector<RawFrame> frames(16);
        size_t count;
        while ((count = walkCurrentThread(layout, frames.data(), frames.size())) > frames.size()) {
            frames.resize(count);
        }

        PyObject* ret = PyList_New(0);
        for (size_t i = 0; ret && i < count; ++i) {
            PyObject* item = Py_BuildValue(
                    "(OiN)",
                    (PyObject*)frames[i].code,
                    frames[i].instruction_offset,
                    PyBool_FromLong(frames[i].is_entry_frame));
            if (!item || PyList_Append(ret, item) < 0) {
                Py_XDECREF(item);
                Py_CLEAR(ret);
                break;
            }
            Py_DECREF(item);
        }
        return ret;
    }

#if PY_VERSION_HEX >= 0x030D0000
    using memray::frame_walker::DebugOffsets;

    PyObject* runtime_debug_offsets_impl()
    {
        const DebugOffsets* table = memray::frame_walker::findRuntimeDebugOffsets();
        if (!table) {
            throw std::runtime_error("_PyRuntime not found");
        }
        return PyBytes_FromStringAndSize(
                reinterpret_cast<const char*>(table), sizeof(DebugOffsets));
    }

    DebugOffsets debug_offsets_from_bytes(const std::string& table)
    {
        DebugOffsets ret;
        if (table.size() != sizeof(ret)) {
            throw std::invalid_argument("debug offsets table has the wrong size");
        }
        memcpy(&ret, table.data(), sizeof(ret));
        return ret;
    }

    std::string validate_debug_offsets_impl(const std::string& table)
    {
        return memray::frame_walker::validateDebugOffsets(debug_offsets_from_bytes(table));
    }

    PyObject* debug_offsets_layout_impl()
    {
        return Py_BuildValue(
                "{s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n,s:n}",
                "cookie", (Py_ssize_t)offsetof(DebugOffsets, cookie),
                "version", (Py_ssize_t)offsetof(DebugOffsets, version),
                "free_threaded", (Py_ssize_t)offsetof(DebugOffsets, free_threaded),
                "thread_state.size", (Py_ssize_t)offsetof(DebugOffsets, thread_state.size),
                "thread_state.current_frame",
                (Py_ssize_t)offsetof(DebugOffsets, thread_state.current_frame),
                "interpreter_frame.size",
                (Py_ssize_t)offsetof(DebugOffsets, interpreter_frame.size),
                "interpreter_frame.previous",
                (Py_ssize_t)offsetof(DebugOffsets, interpreter_frame.previous),
                "interpreter_frame.executable",
                (Py_ssize_t)offsetof(DebugOffsets, interpreter_frame.executable),
                "interpreter_frame.instr_ptr",
                (Py_ssize_t)offsetof(DebugOffsets, interpreter_frame.instr_ptr),
                "interpreter_frame.owner",
                (Py_ssize_t)offsetof(DebugOffsets, interpreter_frame.owner));
    }

    PyObject* debug_offsets_vs_public_headers_impl()
    {
        const DebugOffsets* t = memray::frame_walker::findRuntimeDebugOffsets();
        if (!t) {
            throw std::runtime_error("_PyRuntime not found");
        }
        return Py_BuildValue(
                "{s:(KK),s:(KK),s:(KK),s:(KK),s:(KK),s:(KK)}",
                "thread_state.current_frame",
                (unsigned long long)t->thread_state.current_frame,
                (unsigned long long)offsetof(PyThreadState, current_frame),
                "code_object.filename",
                (unsigned long long)t->code_object.filename,
                (unsigned long long)offsetof(PyCodeObject, co_filename),
                "code_object.name",
                (unsigned long long)t->code_object.name,
                (unsigned long long)offsetof(PyCodeObject, co_name),
                "code_object.linetable",
                (unsigned long long)t->code_object.linetable,
                (unsigned long long)offsetof(PyCodeObject, co_linetable),
                "code_object.firstlineno",
                (unsigned long long)t->code_object.firstlineno,
                (unsigned long long)offsetof(PyCodeObject, co_firstlineno),
                "code_object.co_code_adaptive",
                (unsigned long long)t->code_object.co_code_adaptive,
                (unsigned long long)offsetof(PyCodeObject, co_code_adaptive));
    }

#else
    PyObject* runtime_debug_offsets_impl()
    {
        throw std::runtime_error("no debug offsets table before Python 3.13");
    }

    std::string validate_debug_offsets_impl(const std::string&)
    {
        throw std::runtime_error("no debug offsets table before Python 3.13");
    }

    PyObject* debug_offsets_layout_impl()
    {
        throw std::runtime_error("no debug offsets table before Python 3.13");
    }

    PyObject* debug_offsets_vs_public_headers_impl()
    {
        throw std::runtime_error("no debug offsets table before Python 3.13");
    }
#endif

    PyObject* read_code_object_info_impl(PyObject* code)
    {
        if (!PyCode_Check(code)) {
            throw std::invalid_argument("expected a code object");
        }
        auto info = memray::frame_walker::readCodeObjectInfo((PyCodeObject*)code);
        return Py_BuildValue(
                "(s#s#y#i)",
                info.function_name.data(), (Py_ssize_t)info.function_name.size(),
                info.filename.data(), (Py_ssize_t)info.filename.size(),
                info.linetable.data(), (Py_ssize_t)info.linetable.size(),
                info.firstlineno);
    }
    PyObject* cpython_is_entry_frame_impl(PyObject* frame)
    {
    #if PY_VERSION_HEX >= 0x030B0000
        if (!PyFrame_Check(frame)) {
            throw std::invalid_argument("expected a frame");
        }
        return PyBool_FromLong(_PyFrame_IsEntryFrame((PyFrameObject*)frame));
    #else
        throw std::runtime_error("no entry frames before Python 3.11");
    #endif
    }
    }  // namespace
    """
    object walk_current_thread_impl() except +
    object runtime_debug_offsets_impl() except +
    string validate_debug_offsets_impl(const string&) except +
    object debug_offsets_layout_impl() except +
    object debug_offsets_vs_public_headers_impl() except +
    object read_code_object_info_impl(object) except +
    object cpython_is_entry_frame_impl(object) except +


def walk_current_thread():
    """Walk this thread's frames with memray's frame walker.

    Returns a list of (code object, instruction offset, is entry frame)
    tuples, innermost first.
    """
    return walk_current_thread_impl()


def runtime_debug_offsets():
    """Return the raw bytes of the interpreter's debug offsets table."""
    return runtime_debug_offsets_impl()


def validate_debug_offsets(bytes table):
    """Return why memray would reject a debug offsets table, or ''."""
    return validate_debug_offsets_impl(table)


def debug_offsets_layout():
    """Return the byte position of some fields within the offsets table."""
    return debug_offsets_layout_impl()


def debug_offsets_vs_public_headers():
    """Return {field: (offset from the table, offset from public headers)}."""
    return debug_offsets_vs_public_headers_impl()


def read_code_object_info(code):
    """Read (name, filename, linetable, firstlineno) without using the GIL."""
    return read_code_object_info_impl(code)


def allocate_without_gil(size_t size):
    cdef void* p = NULL
    with nogil:
        p = valloc(size)
    do_not_optimize_ptr(p)
    free(p)


def cpython_is_entry_frame(frame):
    """Ask CPython whether a frame is an entry frame (3.11 and later)."""
    return cpython_is_entry_frame_impl(frame)
