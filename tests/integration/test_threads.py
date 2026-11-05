import sys
import threading
from pathlib import Path

from memray import AllocatorType
from memray import FileReader
from memray import Tracker
from memray._test import MemoryAllocator
from memray._test import set_thread_name
from tests.utils import filter_relevant_allocations
from tests.utils import skip_if_macos

HERE = Path(__file__).parent
TEST_MULTITHREADED_EXTENSION = HERE / "multithreaded_extension"


def allocating_function(allocator, flag_event, wait_event):
    allocator.valloc(1234)
    allocator.free()
    flag_event.set()
    wait_event.wait()
    allocator.valloc(1234)
    allocator.free()


def test_thread_allocations_after_tracker_is_deactivated(tmpdir):
    # GIVEN
    output = Path(tmpdir) / "test.bin"
    wait_event = threading.Event()
    flag_event = threading.Event()
    allocator = MemoryAllocator()

    # WHEN
    with Tracker(output):
        t = threading.Thread(
            target=allocating_function, args=(allocator, flag_event, wait_event)
        )
        t.start()
        flag_event.wait()

    # Keep allocating in the same thread while the tracker is not active
    wait_event.set()
    t.join()

    # THEN
    relevant_records = list(
        filter_relevant_allocations(FileReader(output).get_allocation_records())
    )
    assert len(relevant_records) == 2

    vallocs = [
        record
        for record in relevant_records
        if record.allocator == AllocatorType.VALLOC
    ]
    assert len(vallocs) == 1
    (valloc,) = vallocs
    assert valloc.size == 1234

    frees = [
        record for record in relevant_records if record.allocator == AllocatorType.FREE
    ]
    assert len(frees) == 1


@skip_if_macos
def test_thread_name(tmpdir):
    # GIVEN
    output = Path(tmpdir) / "test.bin"
    allocator = MemoryAllocator()

    def allocating_function():
        set_thread_name("my thread name")
        allocator.valloc(1234)
        allocator.free()

    # WHEN
    with Tracker(output):
        t = threading.Thread(target=allocating_function)
        t.start()
        t.join()

    # THEN
    relevant_records = list(
        filter_relevant_allocations(FileReader(output).get_allocation_records())
    )
    assert len(relevant_records) == 2

    vallocs = [
        record
        for record in relevant_records
        if record.allocator == AllocatorType.VALLOC
    ]
    assert len(vallocs) == 1
    (valloc,) = vallocs
    assert valloc.size == 1234
    assert "my thread name" == valloc.thread_name


def test_setting_python_thread_name(tmpdir):
    # GIVEN
    output = Path(tmpdir) / "test.bin"
    allocator = MemoryAllocator()
    name_set_inside_thread = threading.Event()
    name_set_outside_thread = threading.Event()
    prctl_rc = -1

    def allocating_function():
        allocator.valloc(1234)
        allocator.free()

        threading.current_thread().name = "set inside thread"
        allocator.valloc(1234)
        allocator.free()

        name_set_inside_thread.set()
        name_set_outside_thread.wait()
        allocator.valloc(1234)
        allocator.free()

        nonlocal prctl_rc
        prctl_rc = set_thread_name("set by prctl")
        allocator.valloc(1234)
        allocator.free()

    # WHEN
    with Tracker(output):
        t = threading.Thread(target=allocating_function, name="set before start")
        t.start()
        name_set_inside_thread.wait()
        t.name = "set outside running thread"
        name_set_outside_thread.set()
        t.join()

    # THEN
    expected_names = [
        "set before start",
        "set inside thread",
        "set outside running thread",
        "set by prctl" if prctl_rc == 0 else "set outside running thread",
    ]
    names = [
        rec.thread_name
        for rec in FileReader(output).get_allocation_records()
        if rec.allocator == AllocatorType.VALLOC
    ]
    assert names == expected_names


def test_line_numbers_in_preexisting_thread_before_any_profile_event(tmpdir):
    """Allocations in a thread that existed before tracking started get the
    right line numbers, even before our profile function sees any event."""
    # GIVEN
    output = Path(tmpdir) / "test.bin"
    started = threading.Event()
    go = []

    def allocating_function():
        started.set()
        while not go:  # Spin without making calls, so no profile events fire
            pass
        [None] * 1_000_001  # first allocation
        [None] * 1_000_002  # second allocation

    t = threading.Thread(target=allocating_function)
    t.start()
    assert started.wait(timeout=10)

    # WHEN
    with Tracker(output):
        go.append(True)
        t.join()

    # THEN
    records = sorted(
        (
            record
            for record in FileReader(output).get_allocation_records()
            if record.size >= 8_000_000
        ),
        key=lambda record: record.size,
    )
    source = Path(__file__).read_text().splitlines()
    lines = [source[record.stack_trace()[0][2] - 1] for record in records]
    assert len(lines) == 2
    assert "# first allocation" in lines[0]
    assert "# second allocation" in lines[1]


def test_preexisting_thread_inside_trace_function_when_tracking_starts(tmpdir):
    """A thread's captured stack can include sys.settrace trace function frames
    that our profile function never sees get popped. Make sure they're dropped."""
    # GIVEN
    output = Path(tmpdir) / "test.bin"
    allocator = MemoryAllocator()
    started = threading.Event()
    go = []

    def tracer(frame, event, arg):
        if event == "call" and frame.f_code is traced_function.__code__:
            started.set()
            while not go:
                pass
        return None

    def traced_function():
        allocator.valloc(1234)
        allocator.free()

    def thread_body():
        sys.settrace(tracer)
        traced_function()
        sys.settrace(None)

    t = threading.Thread(target=thread_body)
    t.start()
    assert started.wait(timeout=10)

    # WHEN
    with Tracker(output):
        go.append(True)
        t.join()

    # THEN
    (valloc,) = [
        record
        for record in FileReader(output).get_allocation_records()
        if record.allocator == AllocatorType.VALLOC
    ]
    functions = [frame[0] for frame in valloc.stack_trace()]
    assert "tracer" not in functions
    caller = functions[functions.index("traced_function") + 1]
    assert caller == "thread_body"
