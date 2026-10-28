import importlib.util
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from memray import AllocatorType
from memray import FileReader
from tests.utils import filter_relevant_allocations

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("greenlet") is None, reason="greenlet is not installed"
)


def test_integration_with_greenlet(tmpdir):
    """Verify that we can track Python stacks when greenlet is in use."""
    # GIVEN
    output = Path(tmpdir) / "test.bin"
    subprocess_code = textwrap.dedent(
        f"""
        import greenlet

        from memray import Tracker
        from memray._test import MemoryAllocator


        def apple():
            banana()


        def banana():
            allocator.valloc(1024 * 10)
            animal.switch()
            allocator.valloc(1024 * 30)


        def ant():
            allocator.valloc(1024 * 20)
            fruit.switch()
            allocator.valloc(1024 * 40)
            bat()
            allocator.valloc(1024 * 60)


        def bat():
            allocator.valloc(1024 * 50)


        def test():
            fruit.switch()
            assert fruit.dead
            animal.switch()
            assert animal.dead
            allocator.valloc(1024 * 70)


        allocator = MemoryAllocator()
        output = "{output}"

        with Tracker(output):
            fruit = greenlet.greenlet(apple)
            animal = greenlet.greenlet(ant)
            test()
        """
    )

    # WHEN
    subprocess.run([sys.executable, "-Xdev", "-c", subprocess_code], timeout=5)

    # THEN
    reader = FileReader(output)
    records = list(reader.get_allocation_records())
    vallocs = [
        record
        for record in filter_relevant_allocations(records)
        if record.allocator == AllocatorType.VALLOC
    ]

    def stack(alloc):
        return [frame[0] for frame in alloc.stack_trace()]

    assert stack(vallocs[0]) == ["valloc", "banana", "apple"]
    assert vallocs[0].size == 10 * 1024

    assert stack(vallocs[1]) == ["valloc", "ant"]
    assert vallocs[1].size == 20 * 1024

    assert stack(vallocs[2]) == ["valloc", "banana", "apple"]
    assert vallocs[2].size == 30 * 1024

    assert stack(vallocs[3]) == ["valloc", "ant"]
    assert vallocs[3].size == 40 * 1024

    assert stack(vallocs[4]) == ["valloc", "bat", "ant"]
    assert vallocs[4].size == 50 * 1024

    assert stack(vallocs[5]) == ["valloc", "ant"]
    assert vallocs[5].size == 60 * 1024

    assert stack(vallocs[6]) == ["valloc", "test", "<module>"]
    assert vallocs[6].size == 70 * 1024


def test_importing_greenlet_after_tracking_starts(tmpdir):
    # GIVEN
    output = Path(tmpdir) / "test.bin"
    subprocess_code = textwrap.dedent(
        f"""
        from memray import Tracker
        from memray._test import MemoryAllocator


        def apple():
            banana()


        def banana():
            allocator.valloc(1024 * 10)
            animal.switch()
            allocator.valloc(1024 * 30)


        def ant():
            allocator.valloc(1024 * 20)
            fruit.switch()
            allocator.valloc(1024 * 40)
            bat()
            allocator.valloc(1024 * 60)


        def bat():
            allocator.valloc(1024 * 50)


        def test():
            fruit.switch()
            assert fruit.dead
            animal.switch()
            assert animal.dead
            allocator.valloc(1024 * 70)


        allocator = MemoryAllocator()
        output = "{output}"

        with Tracker(output):
            import greenlet

            fruit = greenlet.greenlet(apple)
            animal = greenlet.greenlet(ant)
            test()
        """
    )

    # WHEN
    subprocess.run([sys.executable, "-Xdev", "-c", subprocess_code], timeout=5)

    # THEN
    reader = FileReader(output)
    records = list(reader.get_allocation_records())
    vallocs = [
        record
        for record in filter_relevant_allocations(records)
        if record.allocator == AllocatorType.VALLOC
    ]

    def stack(alloc):
        return [frame[0] for frame in alloc.stack_trace()]

    assert stack(vallocs[0]) == ["valloc", "banana", "apple"]
    assert vallocs[0].size == 10 * 1024

    assert stack(vallocs[1]) == ["valloc", "ant"]
    assert vallocs[1].size == 20 * 1024

    assert stack(vallocs[2]) == ["valloc", "banana", "apple"]
    assert vallocs[2].size == 30 * 1024

    assert stack(vallocs[3]) == ["valloc", "ant"]
    assert vallocs[3].size == 40 * 1024

    assert stack(vallocs[4]) == ["valloc", "bat", "ant"]
    assert vallocs[4].size == 50 * 1024

    assert stack(vallocs[5]) == ["valloc", "ant"]
    assert vallocs[5].size == 60 * 1024

    assert stack(vallocs[6]) == ["valloc", "test", "<module>"]
    assert vallocs[6].size == 70 * 1024

    # Greenlets don't get their own thread IDs; they all run on one thread.
    assert len({valloc.tid for valloc in vallocs}) == 1


def test_uninstall_profile_in_greenlet(tmpdir):
    """Verify that memray handles profile function changes in greenlets correctly."""
    # GIVEN
    output = Path(tmpdir) / "test.bin"
    subprocess_code = textwrap.dedent(
        f"""
        import greenlet
        import sys

        from memray import Tracker
        from memray._test import MemoryAllocator

        def foo():
            bar()
            allocator.valloc(1024 * 10)

        def bar():
            baz()

        def baz():
            sys.setprofile(None)
            other.switch()

        def test():
            allocator.valloc(1024 * 70)
            main_greenlet.switch()


        allocator = MemoryAllocator()
        output = "{output}"

        with Tracker(output):
            main_greenlet = greenlet.getcurrent()
            other = greenlet.greenlet(test)
            foo()

        """
    )

    # WHEN
    subprocess.run([sys.executable, "-Xdev", "-c", subprocess_code], timeout=5)

    # THEN
    reader = FileReader(output)
    records = list(reader.get_allocation_records())
    vallocs = [
        record
        for record in filter_relevant_allocations(records)
        if record.allocator == AllocatorType.VALLOC
    ]

    def stack(alloc):
        return [frame[0] for frame in alloc.stack_trace()]

    # Memray doesn't use the profile function, so removing it doesn't affect
    # the stacks.
    assert len(vallocs) == 2

    assert stack(vallocs[0]) == ["valloc", "test"]
    assert vallocs[0].size == 70 * 1024

    assert stack(vallocs[1]) == ["valloc", "foo", "<module>"]
    assert vallocs[1].size == 10 * 1024

    # Verify thread IDs
    main_tid = vallocs[0].tid  # inner greenlet
    outer_tid = vallocs[1].tid  # outer greenlet
    assert main_tid == outer_tid


def run_and_get_vallocs(subprocess_code, output):
    proc = subprocess.run([sys.executable, "-Xdev", "-c", subprocess_code], timeout=30)
    assert proc.returncode == 0
    records = list(FileReader(output).get_allocation_records())
    return [
        record
        for record in filter_relevant_allocations(records)
        if record.allocator == AllocatorType.VALLOC
    ]


def stack_of(alloc):
    return [frame[0] for frame in alloc.stack_trace()]


def test_greenlet_switch_while_gen_throw_delegates(tmpdir):
    """Verify stacks when a greenlet switches while gen.throw() is delegating.

    gen.throw() links each generator delegating with ``yield from`` into the
    frame chain without a call event. The delegating generators must not be
    left on the stack once they stop running. Regression test for #1063.
    """
    # GIVEN
    output = Path(tmpdir) / "test.bin"
    subprocess_code = textwrap.dedent(
        f"""
        import sys

        import greenlet

        from memray import Tracker
        from memray._test import MemoryAllocator


        def child():
            main_greenlet.switch()


        def inner():
            worker = greenlet.greenlet(child)
            worker.switch()
            try:
                yield
            except ValueError:
                # The worker dies with the exception and switches back here
                # while gen.throw() is still delegating to this generator.
                worker.throw(*sys.exc_info())


        def middle():
            try:
                yield from inner()
            finally:
                allocator.valloc(1024 * 10)


        def outer():
            yield from middle()


        def test():
            gen = outer()
            next(gen)
            try:
                gen.throw(ValueError)
            except ValueError:
                pass
            allocator.valloc(1024 * 20)


        allocator = MemoryAllocator()
        main_greenlet = greenlet.getcurrent()
        output = "{output}"

        with Tracker(output):
            test()
        """
    )

    # WHEN
    vallocs = run_and_get_vallocs(subprocess_code, output)

    # THEN
    assert len(vallocs) == 2

    # middle is shown once, and outer, which it's still delegating for, below it
    if sys.version_info >= (3, 10):
        assert stack_of(vallocs[0]) == ["valloc", "middle", "outer", "test", "<module>"]
    else:
        # In 3.9, CPython's own frame chain stops at the outermost delegating
        # generator during gen.throw(), so its callers aren't on the stack.
        assert stack_of(vallocs[0]) == ["valloc", "middle", "outer"]
    assert vallocs[0].size == 10 * 1024

    assert stack_of(vallocs[1]) == ["valloc", "test", "<module>"]
    assert vallocs[1].size == 20 * 1024


def test_greenlet_switch_in_cancelled_task(tmpdir):
    """Verify stacks after a task is cancelled while its greenlet is paused.

    This is what SQLAlchemy's asyncio support does when a query times out:
    greenlet_spawn throws the CancelledError into the paused greenlet while
    the task's coroutines are still delegating the throw to it. Regression
    test for #1063.
    """
    # GIVEN
    output = Path(tmpdir) / "test.bin"
    subprocess_code = textwrap.dedent(
        f"""
        import asyncio
        import sys

        import greenlet

        from memray import Tracker
        from memray._test import MemoryAllocator


        async def greenlet_spawn(fn):
            # A minimal version of SQLAlchemy's greenlet_spawn
            worker = greenlet.greenlet(fn, greenlet.getcurrent())
            result = worker.switch()
            while not worker.dead:
                try:
                    value = await result
                except BaseException:
                    result = worker.throw(*sys.exc_info())
                else:
                    result = worker.switch(value)
            return result


        def await_only(awaitable):
            return greenlet.getcurrent().parent.switch(awaitable)


        def query():
            await_only(asyncio.sleep(10))


        async def handler():
            try:
                await asyncio.wait_for(greenlet_spawn(query), 0.01)
            except asyncio.TimeoutError:
                pass


        def after():
            allocator.valloc(1024 * 10)


        async def main():
            await asyncio.gather(handler(), handler())
            after()


        allocator = MemoryAllocator()
        output = "{output}"

        with Tracker(output):
            asyncio.run(main())
        """
    )

    # WHEN
    vallocs = run_and_get_vallocs(subprocess_code, output)

    # THEN
    assert len(vallocs) == 1
    stack = stack_of(vallocs[0])
    assert stack[:3] == ["valloc", "after", "main"]
    assert stack.count("_run_once") == 1
    assert not {"handler", "greenlet_spawn", "wait_for"} & set(stack)


def test_greenlet_suspended_when_tracking_starts(tmpdir):
    """Verify we survive frames that were suspended when tracking started.

    Code that was suspended in a greenlet when tracking started returns
    without a profile event. We must not read those frames once they're
    gone. Regression test for #1063.
    """
    # GIVEN
    output = Path(tmpdir) / "test.bin"
    subprocess_code = textwrap.dedent(
        f"""
        import greenlet

        from memray import Tracker
        from memray._test import MemoryAllocator


        def paused():
            main_greenlet.switch()


        def run():
            paused()
            allocator.valloc(1024 * 10)


        def test():
            worker.switch()
            assert worker.dead
            allocator.valloc(1024 * 20)


        allocator = MemoryAllocator()
        main_greenlet = greenlet.getcurrent()
        output = "{output}"

        worker = greenlet.greenlet(run)
        worker.switch()
        with Tracker(output):
            test()
        """
    )

    # WHEN
    vallocs = run_and_get_vallocs(subprocess_code, output)

    # THEN
    assert len(vallocs) == 2
    assert vallocs[0].size == 10 * 1024
    assert stack_of(vallocs[0]) == ["valloc", "run"]
    assert stack_of(vallocs[1]) == ["valloc", "test", "<module>"]
    assert vallocs[1].size == 20 * 1024


@pytest.mark.skipif(sys.version_info < (3, 11), reason="uses asyncio.timeout")
def test_cancelled_greenlet_spawn_does_not_leave_stale_frames(tmpdir):
    """The reproducer from #1063, with SQLAlchemy's greenlet_spawn pattern.

    Many concurrent calls are cancelled while their greenlets are paused.
    Afterwards, an allocation from the event loop must show the event loop's
    frames exactly once.
    """
    # GIVEN
    output = Path(tmpdir) / "test.bin"
    subprocess_code = textwrap.dedent(
        f"""
        import asyncio
        import sys

        import greenlet

        from memray import Tracker
        from memray._test import MemoryAllocator

        CONCURRENCY = 16


        def await_only(awaitable):
            return greenlet.getcurrent().parent.switch(awaitable)


        async def greenlet_spawn(fn, *args):
            child = greenlet.greenlet(fn, greenlet.getcurrent())
            result = child.switch(*args)
            while not child.dead:
                try:
                    value = await result
                except BaseException:
                    result = child.throw(*sys.exc_info())
                else:
                    result = child.switch(value)
            return result


        def blocking_part(delay):
            await_only(asyncio.sleep(delay))


        async def one_call(i):
            try:
                async with asyncio.timeout(0.002):
                    await greenlet_spawn(blocking_part, 0.001 * i)
            except TimeoutError:
                pass


        def allocate_marker():
            allocator.valloc(1024 * 10)


        async def main():
            for _ in range(50):
                await asyncio.gather(*(one_call(i) for i in range(CONCURRENCY)))
            allocate_marker()


        allocator = MemoryAllocator()
        output = "{output}"

        with Tracker(output):
            asyncio.run(main())
        """
    )

    # WHEN
    vallocs = run_and_get_vallocs(subprocess_code, output)

    # THEN
    assert len(vallocs) == 1
    stack = stack_of(vallocs[0])
    assert stack[:3] == ["valloc", "allocate_marker", "main"]
    assert stack.count("_run_once") == 1
