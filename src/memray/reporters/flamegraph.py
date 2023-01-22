import html
import linecache
import sys
from itertools import tee
from itertools import zip_longest
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import List
from typing import TextIO
from typing import Tuple
from typing import TypeVar

from memray import AllocationRecord
from memray import MemorySnapshot
from memray import Metadata
from memray.reporters.frame_tools import StackFrame
from memray.reporters.frame_tools import is_cpython_internal
from memray.reporters.frame_tools import is_frame_from_import_system
from memray.reporters.frame_tools import is_frame_interesting
from memray.reporters.templates import render_report

MAX_STACKS = int(sys.getrecursionlimit() // 2.5)

T = TypeVar("T")


def pairwise_longest(iterable: Iterator[T]) -> Iterable[Tuple[T, T]]:
    a, b = tee(iterable)
    next(b, None)
    return zip_longest(a, b)


def with_converted_children_dict(node: Dict[str, Any]) -> Dict[str, Any]:
    stack = [node]
    while stack:
        the_node = stack.pop()
        the_node["children"] = [child for child in the_node["children"].values()]
        stack.extend(the_node["children"])
    return node


def create_framegraph_node_from_stack_frame(
    stack_frame: StackFrame, **kwargs: Any
) -> Dict[str, Any]:
    function, filename, lineno = stack_frame

    name = (
        # Use the source file line.
        linecache.getline(filename, lineno)
        # Or just describe where it is from
        or f"{function} at {filename}:{lineno}"
    )
    return {
        "name": name,
        "location": [html.escape(str(part)) for part in stack_frame],
        "value": 0,
        "children": {},
        "n_allocations": 0,
        "thread_id": 0,
        "interesting": (
            is_frame_interesting(stack_frame)
            and not is_frame_from_import_system(stack_frame)
        ),
        **kwargs,
    }


class FlameGraphReporter:
    def __init__(
        self,
        data: Dict[str, Any],
        *,
        memory_records: Iterable[MemorySnapshot],
    ) -> None:
        super().__init__()
        self.data = data
        self.memory_records = memory_records

    @classmethod
    def from_snapshot(
        cls,
        allocations: Iterator[AllocationRecord],
        *,
        memory_records: Iterable[MemorySnapshot],
        native_traces: bool,
    ) -> "FlameGraphReporter":
        data: Dict[str, Any] = {
            "name": "<root>",
            "location": [html.escape("<tracker>"), "<b>memray</b>", 0],
            "value": 0,
            "children": {},
            "n_allocations": 0,
            "thread_id": "0x0",
            "interesting": True,
            "import_system": False,
        }

        unique_threads = set()
        for record in allocations:
            size = record.size
            thread_id = record.thread_name

            data["value"] += size
            data["n_allocations"] += record.n_allocations

            current_frame = data
            stack = (
                tuple(record.hybrid_stack_trace())
                if native_traces
                else record.stack_trace()
            )
            num_skipped_frames = 0
            is_import_system = False
            for index, (stack_frame, next_frame) in enumerate(
                pairwise_longest(reversed(stack))
            ):
                if is_cpython_internal(stack_frame):
                    num_skipped_frames += 1
                    continue
                # Check if the next frame is from the import system. We check
                # the next frame because the "import ..." code will be the parent
                # of the first frame to enter the import system and we want to hide
                # that one as well.
                if is_frame_from_import_system(stack_frame) or (
                    next_frame and is_frame_from_import_system(next_frame)
                ):
                    is_import_system = True
                if (stack_frame, thread_id) not in current_frame["children"]:
                    node = create_framegraph_node_from_stack_frame(
                        stack_frame, import_system=is_import_system
                    )
                    current_frame["children"][(stack_frame, thread_id)] = node

                current_frame = current_frame["children"][(stack_frame, thread_id)]
                current_frame["value"] += size
                current_frame["n_allocations"] += record.n_allocations
                current_frame["thread_id"] = thread_id
                unique_threads.add(thread_id)

                if index - num_skipped_frames > MAX_STACKS:
                    current_frame["name"] = "<STACK TOO DEEP>"
                    current_frame["location"] = ["...", "...", 0]
                    break

        transformed_data = with_converted_children_dict(data)
        transformed_data["unique_threads"] = sorted(unique_threads)
        return cls(transformed_data, memory_records=memory_records)

    @classmethod
    def from_intervals(
        cls,
        intervals: List[Tuple[int, int, AllocationRecord, int, int]],
        *,
        memory_records: Iterable[MemorySnapshot],
        native_traces: bool,
    ) -> "FlameGraphReporter":
        root: Dict[str, Any] = {
            "name": "<root>",
            "location": [html.escape("<tracker>"), "<b>memray</b>", 0],
            "parent": 0,
            "node_id": 0,
            "children": {},
            "thread_id": "0x0",
            "interesting": True,
            "import_system": False,
        }
        nodes = [root]

        unique_threads = set()
        node_by_interval = []
        for interval in intervals:
            record = interval[2]
            thread_id = record.thread_name
            unique_threads.add(thread_id)

            current_frame = root
            stack = tuple(
                reversed(
                    record.hybrid_stack_trace()
                    if native_traces
                    else record.stack_trace()
                )
            )
            num_skipped_frames = 0
            is_import_system = False
            for index, (stack_frame, next_frame) in enumerate(
                pairwise_longest(iter(stack))
            ):
                if is_cpython_internal(stack_frame):
                    num_skipped_frames += 1
                    continue
                # Check if the next frame is from the import system. We check
                # the next frame because the "import ..." code will be the parent
                # of the first frame to enter the import system and we want to hide
                # that one as well.
                if is_frame_from_import_system(stack_frame) or (
                    next_frame and is_frame_from_import_system(next_frame)
                ):
                    is_import_system = True
                if (stack_frame, thread_id) not in current_frame["children"]:
                    node = create_framegraph_node_from_stack_frame(
                        stack_frame, import_system=is_import_system
                    )
                    node["node_id"] = len(nodes)
                    node["parent"] = current_frame["node_id"]
                    node["thread_id"] = thread_id
                    nodes.append(node)
                    current_frame["children"][(stack_frame, thread_id)] = node

                current_frame = current_frame["children"][(stack_frame, thread_id)]

                if index - num_skipped_frames > MAX_STACKS:
                    current_frame["name"] = "<STACK TOO DEEP>"
                    current_frame["location"] = ["...", "...", 0]
                    break

            node_by_interval.append(current_frame)

        assert len(node_by_interval) == len(intervals)

        string_list = []
        string_index_by_value = {}

        def register_string(value):
            idx = string_index_by_value.setdefault(value, len(string_list))
            if idx == len(string_list):
                string_list.append(value)
            return idx

        node_list = [
            (
                register_string(node["name"]),
                register_string(node["location"][0]),
                register_string(node["location"][1]),
                register_string(node["location"][2]),
                node["parent"],
                [c["node_id"] for c in node["children"].values()],
                register_string(node["thread_id"]),
                node["interesting"],
                node["import_system"],
            )
            for node in nodes
        ]
        interval_list = [
            (t0, t1, node["node_id"], n_allocs, n_bytes)
            for node, (t0, t1, _, n_allocs, n_bytes) in zip(node_by_interval, intervals)
        ]
        thread_list = [
            register_string(thread_name) for thread_name in unique_threads
        ]

        data = dict(
            string_list=string_list,
            node_list=node_list,
            interval_list=interval_list,
            thread_list=thread_list,
        )

        return cls(data, memory_records=memory_records)

    def get_html(
        self,
        metadata: Metadata,
        show_memory_leaks: bool,
        merge_threads: bool,
        kind: str = "flamegraph",
        **kwargs: Any,
    ) -> str:
        data = self.data
        if kind == "flamegraph_server":
            data = {
                "name": "<root>",
                "location": [html.escape("<tracker>"), "<b>memray</b>", 0],
                "value": 0,
                "children": [],
                "n_allocations": 0,
                "thread_id": "0x0",
                "interesting": True,
                "import_system": False,
            }
            kwargs["source_data"] = self.data

        return render_report(
            kind=kind,
            data=data,
            metadata=metadata,
            memory_records=self.memory_records,
            show_memory_leaks=show_memory_leaks,
            merge_threads=merge_threads,
            **kwargs,
        )

    def render(
        self,
        outfile: TextIO,
        metadata: Metadata,
        show_memory_leaks: bool,
        merge_threads: bool,
    ) -> None:
        html_code = self.get_html(metadata, show_memory_leaks, merge_threads)
        print(html_code, file=outfile)
