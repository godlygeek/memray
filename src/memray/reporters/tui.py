import os
import threading
from collections import defaultdict
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from functools import total_ordering
from math import ceil
from typing import DefaultDict
from typing import Deque
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple

from rich.markup import escape
from rich.panel import Panel
from rich.progress_bar import ProgressBar
from rich.text import Text
from textual import log
from textual.app import App
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Container
from textual.message import Message
from textual.reactive import reactive
from textual.screen import Screen
from textual.widget import Widget
from textual.widgets import DataTable
from textual.widgets import Footer
from textual.widgets import Label
from textual.widgets import Static

from memray import AllocationRecord
from memray import SocketReader
from memray._memray import size_fmt

MAX_MEMORY_RATIO = 0.95

DEFAULT_TERMINAL_LINES = 24


class SnapshotFetched(Message):
    def __init__(self, snapshot: List[AllocationRecord], disconnected: bool) -> None:
        self.snapshot = snapshot
        self.disconnected = disconnected
        super().__init__()


class MemoryGraph:
    def __init__(
        self,
        width: int,
        height: int,
        minval: float,
        maxval: float,
    ):
        self._graph: List[Deque[str]] = [deque(maxlen=width) for _ in range(height)]
        self.width = width
        self.height = height
        self.minval = minval
        self.maxval = maxval
        self._previous_blocks = [0] * height
        values = [minval] * (2 * self.width + 1)
        self._values = deque(values, maxlen=2 * self.width + 1)
        self.lookup = [
            [" ", "⢀", "⢠", "⢰", "⢸"],
            ["⡀", "⣀", "⣠", "⣰", "⣸"],
            ["⡄", "⣄", "⣤", "⣴", "⣼"],
            ["⡆", "⣆", "⣦", "⣶", "⣾"],
            ["⡇", "⣇", "⣧", "⣷", "⣿"],
        ]

    def _value_to_blocks(self, value: float) -> List[int]:
        dots_per_block = 4
        if value < self.minval:
            n_dots = 0
        elif value > self.maxval:
            n_dots = dots_per_block * self.height
        else:
            n_dots = ceil(
                (value - self.minval)
                / (self.maxval - self.minval)
                * dots_per_block
                * self.height
            )
        blocks = [dots_per_block] * (n_dots // dots_per_block)
        if n_dots % dots_per_block > 0:
            blocks += [n_dots % dots_per_block]
        blocks += [0] * (self.height - len(blocks))
        return blocks

    def add_value(self, value: float) -> None:
        blocks = self._value_to_blocks(value)

        chars = reversed(
            tuple(self.lookup[i0][i1] for i0, i1 in zip(self._previous_blocks, blocks))
        )

        for row, char in enumerate(chars):
            self._graph[row].append(char)

        self._values.append(value)
        self._previous_blocks = blocks

    def reset_max(self, value: float) -> None:
        self._graph = [deque(maxlen=self.width) for _ in range(self.height)]
        self.maxval = value
        for value in self._values.copy():
            self.add_value(value)

    @property
    def graph(self) -> Tuple[str, ...]:
        return tuple("".join(chars) for chars in self._graph)


def _get_terminal_lines() -> int:
    try:
        return os.get_terminal_size().lines
    except OSError:
        return DEFAULT_TERMINAL_LINES


@total_ordering
class SortableText(Text):
    __slots__ = ("value",)

    def __init__(self, value, text, style=""):
        self.value = value
        super().__init__(str(text), style)

    def __lt__(self, other):
        if type(other) != SortableText:
            return NotImplemented
        return self.value < other.value

    def __gt__(self, other):
        if type(other) != SortableText:
            return NotImplemented
        return self.value > other.value

    def __eq__(self, other):
        if type(other) != SortableText:
            return NotImplemented
        return self.value == other.value


def _size_to_color(proportion_of_total: float) -> str:
    if proportion_of_total > 0.6:
        return "red"
    elif proportion_of_total > 0.2:
        return "yellow"
    elif proportion_of_total > 0.05:
        return "green"
    else:
        return "bright_green"


@dataclass(frozen=True, eq=True)
class Location:
    function: str
    file: str


@dataclass
class AllocationEntry:
    own_memory: int
    total_memory: int
    n_allocations: int
    thread_ids: Set[int]


def aggregate_allocations(
    allocations: Iterable[AllocationRecord],
    memory_threshold: float = float("inf"),
    native_traces: Optional[bool] = False,
) -> Dict[Location, AllocationEntry]:
    """Take allocation records and for each frame contained, record "own"
    allocations which happened on the frame, and sum up allocations on
    all of the child frames to calculate "total" allocations."""

    processed_allocations: DefaultDict[Location, AllocationEntry] = defaultdict(
        lambda: AllocationEntry(
            own_memory=0, total_memory=0, n_allocations=0, thread_ids=set()
        )
    )

    current_total = 0
    for allocation in allocations:
        if current_total >= memory_threshold:
            break
        current_total += allocation.size

        stack_trace = list(
            allocation.hybrid_stack_trace()
            if native_traces
            else allocation.stack_trace()
        )
        if not stack_trace:
            frame = processed_allocations[Location(function="???", file="???")]
            frame.total_memory += allocation.size
            frame.own_memory += allocation.size
            frame.n_allocations += allocation.n_allocations
            frame.thread_ids.add(allocation.tid)
            continue
        (function, file_name, _), *caller_frames = stack_trace
        location = Location(function=function, file=file_name)
        processed_allocations[location] = AllocationEntry(
            own_memory=allocation.size,
            total_memory=allocation.size,
            n_allocations=allocation.n_allocations,
            thread_ids={allocation.tid},
        )

        # Walk upwards and sum totals
        visited = set()
        for function, file_name, _ in caller_frames:
            location = Location(function=function, file=file_name)
            frame = processed_allocations[location]
            if location in visited:
                continue
            visited.add(location)
            frame.total_memory += allocation.size
            frame.n_allocations += allocation.n_allocations
            frame.thread_ids.add(allocation.tid)
    return processed_allocations


class TimeDisplay(Static):
    """Widget to display the TUI current time."""

    time = reactive(datetime.now())

    def __init__(self, id):
        super().__init__(id=id)

    def on_mount(self) -> None:
        """Event handler called when widget is added to the app."""
        self.set_interval(1 / 60, self.update_time)

    def update_time(self) -> None:
        """Method to update the time to the current time."""
        self.time = datetime.now()

    def watch_time(self, time: float) -> None:
        """Called when the time attribute changes."""
        self.update(time.ctime().replace(":", "[blink]:[/]"))


class Table(Widget):
    """Widget to display the TUI table."""

    default_sort_column_id = 1
    sort_column_id = reactive(default_sort_column_id)
    max_rows = reactive(None)
    snapshot = reactive(tuple())
    current_thread = reactive(0)
    current_memory_size = reactive(0)

    columns = [
        "Location",
        "Total Memory",
        "Total Memory %",
        "Own Memory",
        "Own Memory % ",
        "Allocation Count",
    ]

    KEY_TO_COLUMN_NAME = {
        1: "total_memory",
        3: "own_memory",
        5: "n_allocations",
    }

    def __init__(self, native: bool):
        super().__init__()
        self._native = native
        self._prev_sort_column_id = 1
        self._terminal_size: int = _get_terminal_lines()

    def on_mount(self):
        log("TUI.on_mount")

    def compose(self) -> ComposeResult:
        log("compose")
        table = DataTable(id="body_table", header_height=2, show_cursor=False)
        for column_idx in range(len(self.columns)):
            if column_idx == self.default_sort_column_id:
                table.add_column(f"<{self.columns[column_idx]}>", key=str(column_idx))
            else:
                table.add_column(self.columns[column_idx], key=str(column_idx))
        yield table

    def watch_current_thread(self) -> None:
        """Called when the current_thread attribute changes."""
        log(f"current_thread updated to {self.current_thread}")
        self.render_table(self.snapshot)

    def watch_snapshot(self) -> None:
        """Called when the snapshot attribute changes."""
        log("watch_snapshot")
        self.render_table(self.snapshot)

    def watch_sort_column_id(self, sort_column_id) -> None:
        """Called when the sort_column_id attribute changes."""
        table = self.query_one("#body_table", DataTable)

        if self._prev_sort_column_id != self.sort_column_id:
            prev_sort_column = table.ordered_columns[self._prev_sort_column_id]
            prev_sort_column.label = self.columns[self._prev_sort_column_id]

            sort_column = table.ordered_columns[sort_column_id]
            sort_column.label = f"<{self.columns[sort_column_id]}>"
            self._prev_sort_column_id = sort_column_id

            table.sort(sort_column.key, reverse=True)

    def render_table(self, snapshot) -> DataTable:
        """Method to render the TUI table."""
        log("rendering table")
        max_rows = self.max_rows or self._terminal_size
        table = self.query_one("#body_table", DataTable)

        if not table.columns:
            log("no columns to render")
            return table

        total_allocations = sum(record.n_allocations for record in snapshot)
        allocation_entries = aggregate_allocations(
            snapshot, MAX_MEMORY_RATIO * self.current_memory_size, self._native
        )

        sorted_allocations = sorted(
            allocation_entries.items(),
            key=lambda item: getattr(  # type: ignore[no-any-return]
                item[1], self.KEY_TO_COLUMN_NAME[self.sort_column_id]
            ),
            reverse=True,
        )[:max_rows]

        # Clear previous table rows
        old_locations = set(table.rows)
        new_locations = set()

        for location, result in sorted_allocations:
            new_locations.add(location)

            if self.current_thread not in result.thread_ids:
                continue
            color_location = (
                f"[bold magenta]{escape(location.function)}[/] at "
                f"[cyan]{escape(location.file)}[/]"
            )
            total_color = _size_to_color(result.total_memory / self.current_memory_size)
            own_color = _size_to_color(result.own_memory / self.current_memory_size)
            allocation_color = _size_to_color(result.n_allocations / total_allocations)

            percent_total = result.total_memory / self.current_memory_size * 100
            percent_own = result.own_memory / self.current_memory_size * 100

            cells = [
                SortableText(
                    result.total_memory, size_fmt(result.total_memory), total_color
                ),
                SortableText(result.total_memory, f"{percent_total:.2f}%", total_color),
                SortableText(result.own_memory, size_fmt(result.own_memory), own_color),
                SortableText(result.own_memory, f"{percent_own:.2f}%", own_color),
                SortableText(
                    result.n_allocations, result.n_allocations, allocation_color
                ),
            ]

            row_key = color_location
            if row_key not in table.rows:
                table.add_row(color_location, *cells, key=row_key)
            else:
                for col_idx, val in enumerate(cells, 1):
                    col_key = str(col_idx)
                    table.update_cell(row_key, col_key, val)

        for row_key in old_locations - new_locations:
            table.remove_row(row_key)

        table.sort(str(self.sort_column_id), reverse=True)

        return table


class Header(Widget):
    """Widget to display TUI header information."""

    pid = reactive("")
    command_line = reactive("")
    n_samples = reactive(0)
    last_update = reactive(datetime.now())
    start = datetime.now()

    def __init__(self, pid: Optional[int], cmd_line: Optional[str]):
        super().__init__()
        self.pid = pid or "???"
        if not cmd_line:
            cmd_line = "???"
        if len(cmd_line) > 50:
            cmd_line = cmd_line[:50] + "..."
        self.command_line = escape(cmd_line)

    def compose(self) -> ComposeResult:
        yield Container(
            Label("\n(∩｀-´)⊃━☆ﾟ.*･｡ﾟ\n"),
            Container(
                Container(
                    Label(f"[b]PID[/]: {self.pid}", id="pid"),
                    Label(id="tid"),
                    Label(id="samples"),
                    id="header_metadata_col_1",
                ),
                Container(
                    Label(f"[b]CMD[/]: {self.command_line}", id="cmd"),
                    Label(id="thread"),
                    Label(id="duration"),
                    id="header_metadata_col_2",
                ),
                id="header_metadata",
            ),
            Static(id="panel"),
            id="header_container",
        )

    def watch_n_samples(self, n_samples: int) -> None:
        """Called when the n_samples attribute changes."""
        self.query_one("#samples", Label).update(f"[b]Samples[/]: {n_samples}")

    def watch_last_update(self, last_update: datetime) -> None:
        """Called when the last_update attribute changes."""
        self.query_one("#duration", Label).update(
            f"[b]Duration[/]: {(last_update - self.start).total_seconds():.1f} seconds"
        )


class HeapSize(Widget):
    """Widget to display TUI heap-size information."""

    current_memory_size = reactive(0)
    max_memory_seen = reactive(0)

    def compose(self) -> ComposeResult:
        yield Container(
            Label(id="current_memory_size"),
            Label(id="max_memory_seen"),
            id="heap_size",
        )
        yield Static(id="progress_bar")

    def update_progress_bar(
        self, current_memory_size: int, max_memory_seen: int
    ) -> None:
        """Method to update the progress bar."""
        self.query_one("#progress_bar", Static).update(
            ProgressBar(
                completed=current_memory_size,
                total=max_memory_seen + 1,
                complete_style="blue",
            )
        )

    def watch_current_memory_size(self, current_memory_size: int) -> None:
        """Called when the current_memory_size attribute changes."""
        self.query_one("#current_memory_size", Label).update(
            f"[bold]Current heap size[/]: {size_fmt(current_memory_size)}"
        )
        self.update_progress_bar(current_memory_size, self.max_memory_seen)

    def watch_max_memory_seen(self, max_memory_seen: int) -> None:
        """Called when the max_memory_seen attribute changes."""
        self.query_one("#max_memory_seen", Label).update(
            f"[bold]Max heap size seen[/]: {size_fmt(max_memory_seen)}"
        )
        self.update_progress_bar(self.current_memory_size, max_memory_seen)


class TUI(Screen):
    """TUI main application class."""

    CSS_PATH = "tui.css"

    TOGGLE_PAUSE_BINDING = Binding("space", "toggle_pause", "Pause")

    BINDINGS = [
        Binding("q,esc", "quit", "Quit"),
        Binding("<,left", "previous_thread", "Previous Thread"),
        Binding(">,right", "next_thread", "Next Thread"),
        Binding("t", "sort(1)", "Sort by Total"),
        Binding("o", "sort(3)", "Sort by Own"),
        Binding("a", "sort(5)", "Sort by Allocations"),
        TOGGLE_PAUSE_BINDING,
    ]

    # Start with a non-empty list of threads so that we always have something
    # to display. This avoids "Thread 1 of 0" and fixes a DivideByZeroError
    # when switching threads before the first allocation is seen.
    _DUMMY_THREAD_LIST = [0]
    stream = MemoryGraph(50, 4, 0.0, 1024.0)

    thread_idx = reactive(0)
    threads = reactive(_DUMMY_THREAD_LIST, always_update=True)
    current_memory_size = reactive(0)
    graph = reactive(stream.graph)
    paused = reactive(False)
    disconnected = reactive(False)
    footer_message = reactive("")

    def __init__(self, pid: Optional[int], cmd_line: Optional[str], native: bool):
        self.pid, self.cmd_line, self.native = pid, cmd_line, native
        self._seen_threads: Set[int] = set()
        self._max_memory_seen = 0

        super().__init__()

    @property
    def current_thread(self) -> int:
        return self.threads[self.thread_idx]

    def action_previous_thread(self) -> None:
        """An action to switch to previous thread."""
        self.thread_idx = (self.thread_idx - 1) % len(self.threads)

    def action_next_thread(self) -> None:
        """An action to switch to next thread."""
        self.thread_idx = (self.thread_idx + 1) % len(self.threads)

    def action_sort(self, col_number: int) -> None:
        """An action to sort the table rows based on a given column attribute."""
        self.update_sort_key(col_number)

    def action_toggle_pause(self) -> None:
        """Toggle pause on keypress"""
        if self.paused or not self.disconnected:
            self.paused = not self.paused
            object.__setattr__(
                self.TOGGLE_PAUSE_BINDING,
                "description",
                "Unpause" if self.paused else "Pause",
            )
            log(self.TOGGLE_PAUSE_BINDING)
            log(self.app.namespace_bindings)
            self.app.query_one(Footer).highlight_key = "space"
            self.app.query_one(Footer).highlight_key = None
            if not self.paused:
                self.display_snapshot()

    def watch_thread_idx(self, thread_idx: int) -> None:
        """Called when the thread_idx attribute changes."""
        self.query_one("#tid", Label).update(f"[b]TID[/]: {hex(self.current_thread)}")
        self.query_one("#thread", Label).update(
            f"[b]Thread[/] {thread_idx + 1} of {len(self.threads)}"
        )
        self.query_one(Table).current_thread = self.current_thread

    def watch_threads(self, threads: List[int]) -> None:
        """Called when the threads attribute changes."""
        self.query_one("#tid", Label).update(f"[b]TID[/]: {hex(self.current_thread)}")
        self.query_one("#thread", Label).update(
            f"[b]Thread[/] {self.thread_idx + 1} of {len(threads)}"
        )

    def watch_current_memory_size(self, current_memory_size: int) -> None:
        """Called when the current_memory_size attribute changes."""
        self.query_one(HeapSize).current_memory_size = current_memory_size
        self.query_one(Table).current_memory_size = current_memory_size

    def watch_graph(self, graph: List[Deque[str]]) -> None:
        """Called when the graph attribute changes to update the header panel."""
        self.query_one("#panel", Static).update(
            Panel(
                "\n".join(graph),
                title="Memory",
                title_align="left",
                border_style="green",
                expand=False,
            )
        )

    def watch_disconnected(self) -> None:
        self.update_label()

    def watch_paused(self) -> None:
        self.update_label()

    def update_label(self) -> None:
        message = []
        if self.paused:
            message.append("[yellow]Paused[/]")
        if self.disconnected:
            message.append("[red]Remote has disconnected[/]")
        self.query_one("#message", Label).update(" ".join(message))

    def compose(self) -> ComposeResult:
        yield Container(
            Label("[b]Memray[/b] live tracking", id="head_title"),
            TimeDisplay(id="head_time_display"),
            id="head",
        )
        yield Header(pid=self.pid, cmd_line=escape(self.cmd_line))
        yield HeapSize()
        yield Table(native=self.native)
        yield Label(id="message")
        yield Footer()

    def get_header(self):
        return self.query_one("head", Container) + "\n" + self.query_one(Header)

    def get_body(self):
        return self.query_one(Table)

    def get_heap_size(self):
        return self.query_one(HeapSize)

    def update_snapshot(self, snapshot: Iterable[AllocationRecord]) -> None:
        """Method called to update snapshot."""
        self._latest_snapshot = tuple(snapshot)
        self.display_snapshot()

    def display_snapshot(self) -> None:
        snapshot = self._latest_snapshot

        header = self.query_one(Header)
        heap = self.query_one(HeapSize)
        body = self.query_one(Table)

        # We want to update many header fields even when paused
        header.n_samples += 1
        header.last_update = datetime.now()

        self.current_memory_size = sum(record.size for record in snapshot)
        if self.current_memory_size > heap.max_memory_seen:
            heap.max_memory_seen = self.current_memory_size
            self.stream.reset_max(heap.max_memory_seen)
        self.stream.add_value(self.current_memory_size)
        self.graph = self.stream.graph

        # Other fields should only be updated when not paused.
        if self.paused:
            return

        new_tids = {record.tid for record in snapshot} - self._seen_threads
        self._seen_threads.update(new_tids)

        if new_tids:
            threads = self.threads
            if threads is self._DUMMY_THREAD_LIST:
                threads = []
            for tid in sorted(new_tids):
                threads.append(tid)
            self.threads = threads

        body.current_thread = self.current_thread
        if not self.paused:
            log("Updating snapshot!")
            body.snapshot = snapshot

    def update_sort_key(self, col_number: int) -> None:
        """Method called to update the table sort key attribute."""
        body = self.query_one(Table)
        body.sort_column_id = col_number


class UpdateThread(threading.Thread):
    def __init__(self, app: App, reader: SocketReader) -> None:
        self._app = app
        self._reader = reader
        self._update_requested = threading.Event()
        self._canceled = threading.Event()
        super().__init__()

    def run(self) -> None:
        while self._update_requested.wait():
            if self._canceled.is_set():
                return
            self._update_requested.clear()
            self._app.post_message(
                SnapshotFetched(
                    list(self._reader.get_current_snapshot(merge_threads=False)),
                    not self._reader.is_active,
                )
            )

            if not self._reader.is_active:
                return

    def cancel(self):
        self._canceled.set()
        self._update_requested.set()

    def schedule_update(self) -> None:
        self._update_requested.set()


class TUIApp(App):
    """TUI main application class."""

    CSS_PATH = "tui.css"

    def __init__(self, reader: SocketReader):
        self._reader = reader
        self.active = True
        super().__init__()

    def on_mount(self):
        self.update_thread = UpdateThread(self, self._reader)
        self.update_thread.start()

        self.set_interval(0.1, self.update_thread.schedule_update)
        self.push_screen(
            TUI(
                pid=self._reader.pid,
                cmd_line=self._reader.command_line,
                native=self._reader.has_native_traces,
            )
        )
        log(self.namespace_bindings)

    def on_unmount(self):
        self.update_thread.cancel()
        self.update_thread.join()
        log("!!! THREAD JOINED")

    def on_snapshot_fetched(self, message: SnapshotFetched) -> None:
        """Method called to process each fetched snapshot."""
        log("on snapshot fetched")
        tui = self.query_one(TUI)
        tui.update_snapshot(message.snapshot)
        if message.disconnected:
            self.active = False
            log("setting disconnected")
            tui.disconnected = True
