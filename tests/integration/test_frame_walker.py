"""Tests for the frame walker that reads a thread's Python stack directly."""

import asyncio
import importlib.util
import struct
import sys
import sysconfig
import threading

import pytest

from memray import Tracker
from memray._memray import _set_debug_offsets_override_for_testing
from memray._test_utils import cpython_is_entry_frame
from memray._test_utils import debug_offsets_layout
from memray._test_utils import debug_offsets_vs_public_headers
from memray._test_utils import read_code_object_info
from memray._test_utils import runtime_debug_offsets
from memray._test_utils import validate_debug_offsets
from memray._test_utils import walk_current_thread

HAS_GREENLET = importlib.util.find_spec("greenlet") is not None
FREE_THREADED = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))

needs_debug_offsets = pytest.mark.skipif(
    sys.version_info < (3, 13), reason="the debug offsets table is new in 3.13"
)
needs_entry_frames = pytest.mark.skipif(
    sys.version_info < (3, 11), reason="every frame is an entry frame before 3.11"
)


def walked_offset_to_f_lasti(offset):
    # In 3.10, the walker reports the raw f_lasti, which counts instructions,
    # but the f_lasti attribute is in bytes. Other versions use bytes for both.
    if sys.version_info[:2] == (3, 10) and offset >= 0:
        return offset * 2
    return offset


def capture():
    """Walk the stack, and collect the same frames through the public API.

    Returns (walked, expected): two lists of (code object, f_lasti), for the
    frames above this function, innermost first.
    """
    walked = walk_current_thread()
    assert walked[0][0] is capture.__code__
    walked = [(code, walked_offset_to_f_lasti(offset)) for code, offset, _ in walked[1:]]

    expected = []
    frame = sys._getframe(1)
    while frame is not None:
        expected.append((frame.f_code, frame.f_lasti))
        frame = frame.f_back
    return walked, expected


def names(frames):
    return [code.co_name for code, _ in frames]


def check(result):
    walked, expected = result
    assert names(walked) == names(expected)
    assert walked == expected
    return walked


def test_plain_calls():
    def inner():
        return capture()

    def outer():
        return inner()

    walked = check(outer())
    assert names(walked)[:3] == ["inner", "outer", "test_plain_calls"]


def test_recursive_calls():
    def recurse(n):
        if n == 0:
            return capture()
        return recurse(n - 1)

    walked = check(recurse(50))
    assert names(walked)[:51] == ["recurse"] * 51


def test_generator():
    def gen():
        yield capture()

    walked = check(next(gen()))
    assert names(walked)[:2] == ["gen", "test_generator"]


def test_nested_generators_with_yield_from():
    def inner():
        yield capture()

    def middle():
        yield from inner()

    def outer():
        yield from middle()

    walked = check(next(outer()))
    assert names(walked)[:4] == ["inner", "middle", "outer", "test_nested_generators_with_yield_from"]


def test_generator_throw_delegation():
    def inner():
        try:
            yield
        except ValueError:
            yield capture()

    def outer():
        yield from inner()

    gen = outer()
    next(gen)
    walked = check(gen.throw(ValueError))
    assert names(walked)[:2] == ["inner", "outer"]
    if sys.version_info >= (3, 10):
        # In 3.9, CPython's own chain stops at the outermost delegating
        # generator during gen.throw(); check() verified that we match it.
        assert names(walked)[2] == "test_generator_throw_delegation"


def test_coroutine():
    async def inner():
        return capture()

    async def outer():
        return await inner()

    coro = outer()
    with pytest.raises(StopIteration) as exc_info:
        coro.send(None)
    walked = check(exc_info.value.value)
    assert names(walked)[:3] == ["inner", "outer", "test_coroutine"]


def test_asyncio_and_async_generator():
    async def agen():
        yield capture()

    async def main():
        async for result in agen():
            return result

    # Use a private loop: asyncio.run() unsets the current event loop, which
    # breaks later tests that call asyncio.get_event_loop() on Python 3.9.
    loop = asyncio.new_event_loop()
    try:
        walked = check(loop.run_until_complete(main()))
    finally:
        loop.close()
    assert names(walked)[:2] == ["agen", "main"]


def test_exception_in_flight():
    def inner():
        try:
            raise ValueError
        except ValueError:
            return capture()

    walked = check(inner())
    assert names(walked)[0] == "inner"


def test_finally_block():
    def inner():
        try:
            raise ValueError
        finally:
            return capture()  # noqa: B012

    walked = check(inner())
    assert names(walked)[0] == "inner"


def test_class_body():
    results = []

    class Foo:
        results.append(capture())

    walked = check(results[0])
    assert names(walked)[:2] == ["Foo", "test_class_body"]


def test_decorator_and_lambda():
    def decorator(func):
        def wrapper():
            return func()

        return wrapper

    @decorator
    def decorated():
        return (lambda: capture())()

    walked = check(decorated())
    assert names(walked)[:4] == ["<lambda>", "decorated", "wrapper", "test_decorator_and_lambda"]


def test_comprehension():
    walked = check([capture() for _ in range(1)][0])
    # Comprehensions are inlined into their enclosing function from 3.12.
    assert names(walked)[0] in ("test_comprehension", "<listcomp>")


@needs_entry_frames
def test_called_from_c():
    walked = walk_current_thread()
    assert walked[-1][2], "the outermost frame should be an entry frame"

    results = list(map(lambda _: walk_current_thread(), [None]))
    lambda_frame, caller_frame = results[0][0], results[0][1]
    assert lambda_frame[0].co_name == "<lambda>"
    assert lambda_frame[2], "a function called from C is an entry frame"
    assert caller_frame[0].co_name == "test_called_from_c"


def check_entry_flags():
    """Compare the walker's entry frame flags with CPython's."""
    walked = walk_current_thread()[1:]
    expected = []
    frame = sys._getframe(1)
    while frame is not None:
        expected.append((frame.f_code, cpython_is_entry_frame(frame)))
        frame = frame.f_back
    assert [(code, is_entry) for code, _, is_entry in walked] == expected
    return expected


@needs_entry_frames
def test_entry_frame_flags_match_cpython():
    def gen():
        yield check_entry_flags()

    class WithInit:
        def __init__(self):
            self.flags = check_entry_flags()

    results = [
        check_entry_flags(),
        list(map(lambda _: check_entry_flags(), [None]))[0],
        next(gen()),
        WithInit().flags,
        sorted([2, 1], key=lambda x: (check_entry_flags(), x)[1]) and check_entry_flags(),
    ]

    # Make sure both kinds of frames were seen.
    flags = [is_entry for result in results for _, is_entry in result]
    assert True in flags
    assert False in flags

def test_thread():
    results = []

    def thread_body():
        results.append(capture())

    thread = threading.Thread(target=thread_body)
    thread.start()
    thread.join()

    walked = check(results[0])
    assert names(walked)[0] == "thread_body"
    assert "test_thread" not in names(walked)


@pytest.mark.skipif(not HAS_GREENLET, reason="greenlet is not installed")
def test_greenlet():
    import greenlet

    results = []

    def inner():
        results.append(capture())
        main.switch()
        results.append(capture())

    def run():
        inner()

    main = greenlet.getcurrent()
    other = greenlet.greenlet(run)
    other.switch()
    other.switch()

    for result in results:
        walked = check(result)
        assert names(walked)[:2] == ["inner", "run"]


def test_reading_code_object_info():
    def fün():
        pass

    code = fün.__code__
    assert read_code_object_info(code) == (
        code.co_name,
        code.co_filename,
        code.co_linetable if sys.version_info >= (3, 10) else code.co_lnotab,
        code.co_firstlineno,
    )


@pytest.mark.parametrize(
    "name",
    ["ascii", "latin1_é", "bmp_函数", "astral_😀", ""],
)
def test_reading_non_ascii_names(name):
    code = (lambda: None).__code__.replace(co_name=name, co_filename=f"/tmp/{name}.py")
    assert read_code_object_info(code)[:2] == (name, f"/tmp/{name}.py")


def test_reading_lone_surrogates():
    code = (lambda: None).__code__.replace(co_name="a\ud800b")
    assert read_code_object_info(code)[0] == "a�b"


@needs_debug_offsets
def test_debug_offsets_match_public_headers():
    for field, (from_table, from_headers) in debug_offsets_vs_public_headers().items():
        assert from_table == from_headers, field


@needs_debug_offsets
def test_runtime_debug_offsets_are_valid():
    assert validate_debug_offsets(runtime_debug_offsets()) == ""


def corrupt(field, value):
    """Return the runtime offsets table with one field changed."""
    table = bytearray(runtime_debug_offsets())
    position = debug_offsets_layout()[field]
    if isinstance(value, bytes):
        table[position : position + len(value)] = value
    else:
        struct.pack_into("=Q", table, position, value)
    return bytes(table)


def bad_tables():
    table = runtime_debug_offsets()
    layout = debug_offsets_layout()

    def get(field):
        return struct.unpack_from("=Q", table, layout[field])[0]

    frame_size = get("interpreter_frame.size")
    return {
        "cookie": corrupt("cookie", b"xdebugpz"),
        "version": corrupt("version", 0x030C0000),
        "free_threaded": corrupt("free_threaded", 0 if FREE_THREADED else 1),
        "thread_state_size": corrupt("thread_state.size", 0),
        "current_frame": corrupt("thread_state.current_frame", get("thread_state.size")),
        "frame_size": corrupt("interpreter_frame.size", 0),
        "previous": corrupt("interpreter_frame.previous", frame_size),
        "executable": corrupt("interpreter_frame.executable", frame_size),
        "instr_ptr": corrupt("interpreter_frame.instr_ptr", frame_size),
        "owner": corrupt("interpreter_frame.owner", frame_size + 100),
    }


BAD_TABLES = [
    "cookie",
    "version",
    "free_threaded",
    "thread_state_size",
    "current_frame",
    "frame_size",
    "previous",
    "executable",
    "instr_ptr",
    "owner",
]


def test_bad_tables_are_all_tested():
    if sys.version_info >= (3, 13):
        assert sorted(bad_tables()) == sorted(BAD_TABLES)


@needs_debug_offsets
@pytest.mark.parametrize("which", BAD_TABLES)
def test_bad_debug_offsets_are_rejected(which):
    assert validate_debug_offsets(bad_tables()[which]) != ""



@needs_debug_offsets
def test_tracker_refuses_to_start_with_bad_debug_offsets(tmp_path):
    # GIVEN
    _set_debug_offsets_override_for_testing(bad_tables()["version"])

    # WHEN / THEN
    try:
        with pytest.raises(RuntimeError, match="unsupported Python version"):
            with Tracker(tmp_path / "bad.bin"):
                pass  # pragma: no cover
    finally:
        _set_debug_offsets_override_for_testing(None)

    # A tracker can still be started afterwards.
    with Tracker(tmp_path / "good.bin"):
        pass
