import subprocess
import sys
import textwrap
from pathlib import Path

from memray import AllocatorType
from memray import FileReader
from tests.utils import filter_relevant_allocations


def test_integration_with_greenlet(tmpdir):
    """Verify that we can track Python stacks when greenlet is in use."""
    # GIVEN
    output = Path(tmpdir) / "test.bin"
    subprocess_code = textwrap.dedent(
        f"""
        import mmap
        import sys
        import gc

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
        import mmap
        import sys
        import gc

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
