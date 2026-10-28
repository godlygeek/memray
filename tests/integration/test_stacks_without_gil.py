"""Check the Python stacks reported for allocations made without the GIL."""

import importlib.util
import threading

import pytest

from memray import AllocatorType
from memray import FileReader
from memray import Tracker
from memray._test_utils import allocate_without_gil

SIZE = 123_457

HAS_GREENLET = importlib.util.find_spec("greenlet") is not None


def allocations_of_marker_size(output):
    return [
        record
        for record in FileReader(output).get_allocation_records()
        if record.allocator == AllocatorType.VALLOC and record.size == SIZE
    ]


def functions(record):
    return [frame[0] for frame in record.stack_trace()]


def lines(record):
    return [frame[2] for frame in record.stack_trace()]


def python_frames(record):
    # allocate_without_gil is a Cython function, which has no Python frame.
    stack = functions(record)
    assert "allocate_without_gil" not in stack
    return stack


def test_deep_stack(tmp_path):
    # GIVEN
    output = tmp_path / "test.bin"

    def recurse(n):
        if n == 0:
            allocate_without_gil(SIZE)
        else:
            recurse(n - 1)

    # WHEN
    with Tracker(output):
        recurse(200)

    # THEN
    (record,) = allocations_of_marker_size(output)
    stack = python_frames(record)
    assert stack[:202] == ["recurse"] * 201 + ["test_deep_stack"]
    expected_line = recurse.__code__.co_firstlineno + 2
    assert lines(record)[0] == expected_line
    assert lines(record)[1:201] == [expected_line + 2] * 200


def test_generator(tmp_path):
    # GIVEN
    output = tmp_path / "test.bin"

    def gen():
        yield
        allocate_without_gil(SIZE)
        yield

    # WHEN
    with Tracker(output):
        it = gen()
        next(it)
        next(it)

    # THEN
    (record,) = allocations_of_marker_size(output)
    assert python_frames(record)[:2] == ["gen", "test_generator"]
    assert lines(record)[0] == gen.__code__.co_firstlineno + 2


@pytest.mark.skipif(not HAS_GREENLET, reason="greenlet is not installed")
def test_greenlet(tmp_path):
    # GIVEN
    import greenlet

    output = tmp_path / "test.bin"

    def inner():
        main.switch()
        allocate_without_gil(SIZE)

    def run():
        inner()

    # WHEN
    main = greenlet.getcurrent()
    other = greenlet.greenlet(run)
    other.switch()
    with Tracker(output):
        other.switch()

    # THEN
    (record,) = allocations_of_marker_size(output)
    assert python_frames(record) == ["inner", "run"]
    assert lines(record)[0] == inner.__code__.co_firstlineno + 2


def test_thread_started_before_tracking(tmp_path):
    # GIVEN
    output = tmp_path / "test.bin"
    tracking_started = threading.Event()

    def thread_body():
        tracking_started.wait()
        allocate_in_thread()

    def allocate_in_thread():
        allocate_without_gil(SIZE)

    thread = threading.Thread(target=thread_body)
    thread.start()

    # WHEN
    with Tracker(output):
        tracking_started.set()
        thread.join()

    # THEN
    (record,) = allocations_of_marker_size(output)
    stack = python_frames(record)
    assert stack[:2] == ["allocate_in_thread", "thread_body"]
    assert "test_thread_started_before_tracking" not in stack
    assert lines(record)[0] == allocate_in_thread.__code__.co_firstlineno + 1
