#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <assert.h>
#include <pthread.h>

#ifdef __linux__
#include <malloc.h>
#endif

namespace {  // unnamed

#pragma GCC push_options
#pragma GCC optimize ("O0")

const int NUM_THREADS = 100;
const int NUM_BUFFERS = 100;
pthread_t threads[NUM_THREADS];

extern "C" void
allocate_memory()
{
    unsigned long* buffers[NUM_BUFFERS];
    for (int i=0; i < NUM_BUFFERS; ++i) {
        int ret = posix_memalign((void**)buffers+i, sizeof(void*), sizeof(void*)*(i+1));
        if (ret) {
            buffers[i] = NULL;
            break;
        }
    }
    for (int i=0; i < NUM_BUFFERS; ++i) {
        free(buffers[i]);
    }
}

extern "C" void*
worker(void*)
{
    allocate_memory();
    return NULL;
}

void start_threads()
{
    for (int i=0; i<NUM_THREADS; ++i) {
        pthread_t thread;
        int ret = pthread_create(&thread, NULL, &worker, NULL);
        assert(0 == ret);
        threads[i] = thread;
    }
}

void join_threads()
{
    for (int i=0; i<NUM_THREADS; ++i) {
        pthread_join(threads[i], NULL);
    }
}

__attribute__((optnone)) static void cleanup_handler(void* arg) {
  void* data = valloc(sizeof(int));
  free(data);
}

static void* create_tls_in_thread(void *arg) {
  pthread_key_t thread_specific_storage;
  pthread_key_create(&thread_specific_storage, cleanup_handler);
  pthread_setspecific(thread_specific_storage, (void*)12);
  pthread_exit(NULL);
  return NULL;
}

void valloc_on_thread_exit() {
  pthread_t thread_id;
  void *result;
  pthread_create(&thread_id, NULL, create_tls_in_thread, NULL);
  pthread_join(thread_id, &result);
}

static void call_with_gil(PyObject* callback) {
  PyGILState_STATE gstate = PyGILState_Ensure();
  PyObject* ret = PyObject_CallNoArgs(callback);
  if (!ret) {
      PyErr_Print();
  }
  Py_XDECREF(ret);
  PyGILState_Release(gstate);
}

// Calls a Python function from the destructor of a C++ thread_local, which
// may run after Memray's own thread-local state was destroyed.
struct CallOnThreadExit
{
    PyObject* callback = nullptr;

    ~CallOnThreadExit()
    {
        call_with_gil(callback);
    }
};

static void* call_from_thread_local_destructor(void* callback) {
  // Construct the thread_local before Memray's shadow stack is created for
  // this thread, so that it's destroyed after the shadow stack is.
  static thread_local CallOnThreadExit call_on_exit;
  call_on_exit.callback = static_cast<PyObject*>(callback);
  call_with_gil(call_on_exit.callback);
  return NULL;
}

// Calls a Python function from a pthread key destructor. The key is created
// when this module is imported, after Memray's key, so its destructor runs
// after Memray's (glibc runs them in key creation order).
static pthread_key_t call_on_exit_key;

static void call_on_exit_key_destructor(void* callback) {
  call_with_gil(static_cast<PyObject*>(callback));
}

static void* call_from_pthread_key_destructor(void* callback) {
  pthread_setspecific(call_on_exit_key, callback);
  call_with_gil(static_cast<PyObject*>(callback));
  return NULL;
}

PyObject*
run_python_at_thread_exit(PyObject*, PyObject* args)
{
    PyObject* callback;
    int use_pthread_key;
    if (!PyArg_ParseTuple(args, "Op", &callback, &use_pthread_key)) {
        return NULL;
    }

    pthread_t thread_id;
    Py_BEGIN_ALLOW_THREADS
    pthread_create(
            &thread_id,
            NULL,
            use_pthread_key ? call_from_pthread_key_destructor : call_from_thread_local_destructor,
            callback);
    pthread_join(thread_id, NULL);
    Py_END_ALLOW_THREADS
    Py_RETURN_NONE;
}

PyObject*
run(PyObject*, PyObject*)
{
    start_threads();
    join_threads();
    Py_RETURN_NONE;
}

PyObject*
run_valloc_at_exit(PyObject*, PyObject*)
{
    valloc_on_thread_exit();
    Py_RETURN_NONE;
}

#pragma GCC pop_options

}  // unnamed namespace

static PyMethodDef methods[] = {
        {"run", run, METH_NOARGS, "Run a bunch of threads"},
        {"run_valloc_at_exit", run_valloc_at_exit, METH_NOARGS, "Run valloc while exiting a thread"},
        {"run_python_at_thread_exit",
         run_python_at_thread_exit,
         METH_VARARGS,
         "Call a function in a new thread, and again from a TLS destructor"},
        {NULL, NULL, 0, NULL},
};

static struct PyModuleDef moduledef = {PyModuleDef_HEAD_INIT, "testext", "", -1, methods};

PyMODINIT_FUNC
PyInit_testext(void)
{
    if (0 != pthread_key_create(&call_on_exit_key, call_on_exit_key_destructor)) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create pthread key");
        return NULL;
    }
    PyObject *mod = PyModule_Create(&moduledef);
#ifdef Py_GIL_DISABLED
    PyUnstable_Module_SetGIL(mod, Py_MOD_GIL_NOT_USED);
#endif
    return mod;
}
