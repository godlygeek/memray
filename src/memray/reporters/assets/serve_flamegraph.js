import { debounced } from "./common";

import {
  initThreadsDropdown,
  drawChart,
  handleFragments,
  onFilterUninteresting,
  onFilterImportSystem,
  onFilterThread,
  onResetZoom,
  onResize,
  onInvert,
  getFilteredChart,
  getFlamegraph,
} from "./flamegraph_common";

var active_plots = [];
var current_dimensions = null;

function initMemoryGraph(memory_records) {
  console.log("init memory graph");
  const time = memory_records.map((a) => new Date(a[0]));
  const resident_size = memory_records.map((a) => a[1]);
  const heap_size = memory_records.map((a) => a[2]);

  var resident_size_plot = {
    x: time,
    y: resident_size,
    mode: "lines",
    name: "Resident size",
  };

  var heap_size_plot = {
    x: time,
    y: heap_size,
    mode: "lines",
    name: "Heap size",
  };

  var plot_data = [resident_size_plot, heap_size_plot];
  var config = {
    responsive: true,
    displayModeBar: false,
  };
  var layout = {
    xaxis: {
      title: {
        text: "Time",
      },
      rangeslider: {
        visible: true,
      },
    },
    yaxis: {
      title: {
        text: "Memory Size",
      },
      tickformat: ".4~s",
      exponentformat: "B",
      ticksuffix: "B",
    },
  };

  Plotly.newPlot("plot", plot_data, layout, config).then((plot) => {
    console.assert(active_plots.length == 0);
    active_plots.push(plot);
  });
}

function showLoadingAnimation() {
  console.log("showLoadingAnimation");
  document.getElementById("loading").style.display = "block";
  document.getElementById("overlay").style.display = "block";
}

function hideLoadingAnimation() {
  console.log("hideLoadingAnimation");
  document.getElementById("loading").style.display = "none";
  document.getElementById("overlay").style.display = "none";
}

function refreshFlamegraph(event) {
  console.log("refreshing flame graph!");

  let request_data = getRangeData(event);

  if (
    current_dimensions != null &&
    JSON.stringify(request_data) === JSON.stringify(current_dimensions)
  ) {
    return;
  }

  console.log("showing loading animation");
  showLoadingAnimation();

  current_dimensions = request_data;

  console.log("finding range of relevant snapshot");
  let t0 = new Date(request_data.string1).getTime();
  let t1 = new Date(request_data.string2).getTime();

  let idx0 = 0;
  for (let i = 0; i < memory_records.length; i++) {
    if (memory_records[i][0] >= t0) {
      idx0 = i + 1;
      break;
    }
  }

  let idx1 = memory_records.length + 1;
  for (let i = memory_records.length - 1; i >= 0; i--) {
    if (memory_records[i][0] < t1) {
      idx1 = i + 1;
      break;
    }
  }

  console.log("start index is " + idx0);
  console.log("end index is " + idx1);

  let total_allocations_by_node = new Array(source_data.node_list.length).fill(0);
  let total_bytes_by_node = new Array(source_data.node_list.length).fill(0);

  // FIXME we should binary search to find the range of interesting intervals.
  console.log("finding leaked allocations");
  source_data.interval_list.forEach(interval => {
    let allocated_before = interval[0];
    let deallocated_before = interval[1];
    let node_index = interval[2];
    let num_allocations = interval[3];
    let num_bytes = interval[4];

    if (allocated_before >= idx0 && allocated_before <= idx1 && deallocated_before > idx1) {
      while (true) {
        total_allocations_by_node[node_index] += num_allocations;
        total_bytes_by_node[node_index] += num_bytes;
        if (node_index == 0) {
          break;
        }
        node_index = source_data.node_list[node_index][4];
      }
    }
  });

  console.log("total leaked allocations in range: " + total_allocations_by_node[0]);
  console.log("total leaked bytes in range: " + total_bytes_by_node[0]);

  console.log("constructing tree");
  let node_objects = new Array(source_data.node_list.length);
  for (let i = 0; i < source_data.node_list.length; ++i) {
    if (total_allocations_by_node[i] == 0 && i != 0) {
      continue;
    }
    let fields = source_data.node_list[i];

    let name_idx = fields[0];
    let location0_idx = fields[1];
    let location1_idx = fields[2];
    let location2_idx = fields[3];
    let parent_idx = fields[4];
    let child_idxs = fields[5];
    let tid_idx = fields[6];
    let interesting = fields[7];
    let import_system = fields[8];

    node_objects[i] = {
      "name": source_data.string_list[name_idx],
      "location": [
        source_data.string_list[location0_idx],
        source_data.string_list[location1_idx],
        source_data.string_list[location2_idx],
      ],
      "value": total_bytes_by_node[i],
      "children": child_idxs,
      "n_allocations": total_allocations_by_node[i],
      "thread_id": source_data.string_list[tid_idx],
      "interesting": interesting,
      "import_system": import_system,
    };
  }

  node_objects.forEach(node => {
    if (node) {
      node.children = node.children.map(node_idx => node_objects[node_idx]);
      node.children = node.children.filter(node => node && node.n_allocations > 0);
    }
  });

  console.log("drawing chart");
  data = node_objects[0];
  getFilteredChart().drawChart(data);
  console.log("hiding loading animation");
  hideLoadingAnimation();
}

function getRangeData(event) {
  console.log("getRangeData");
  let request_data = {};
  if (event.hasOwnProperty("xaxis.range[0]")) {
    request_data = {
      string1: event["xaxis.range[0]"],
      string2: event["xaxis.range[1]"],
    };
  } else if (event.hasOwnProperty("xaxis.range")) {
    request_data = {
      string1: event["xaxis.range"][0],
      string2: event["xaxis.range"][1],
    };
  } else if (active_plots.length == 1) {
    let the_range = active_plots[0].layout.xaxis.range;
    request_data = {
      string1: the_range[0],
      string2: the_range[1],
    };
  } else {
    return;
  }
  return request_data;
}

var debounce = null;
function refreshFlamegraphDebounced(event) {
  console.log("refreshFlamegraphDebounced");
  if (debounce) {
    clearTimeout(debounce);
  }
  debounce = setTimeout(function () {
    refreshFlamegraph(event);
  }, 500);
}

// Main entrypoint
function main() {
  console.log("main");
  initThreadsDropdown(data, merge_threads);

  initMemoryGraph(memory_records);

  // Create the flamegraph renderer
  console.log("whoopsie!!");
  //drawChart(data);
  console.log("done whoopsie.");

  // Set zoom to correct element
  console.log("handleFragments");
  if (location.hash) {
    handleFragments();
  }

  // Setup event handlers
  console.log("setup event handlers");
  document.getElementById("invertButton").onclick = onInvert;
  document.getElementById("resetZoomButton").onclick = onResetZoom;
  document.getElementById("resetThreadFilterItem").onclick = onFilterThread;
  let hideUninterestingCheckBox = document.getElementById("hideUninteresting");
  hideUninterestingCheckBox.onclick = onFilterUninteresting.bind(this);
  let hideImportSystemCheckBox = document.getElementById("hideImportSystem");
  hideImportSystemCheckBox.onclick = onFilterImportSystem.bind(this);
  // Enable filtering by default
  console.log("onFilterUninteresting");
  onFilterUninteresting.bind(this)();

  console.log("onkeyup");
  document.onkeyup = (event) => {
    if (event.code == "Escape") {
      onResetZoom();
    }
  };
  console.log("searchTerm listener");
  document.getElementById("searchTerm").addEventListener("input", () => {
    const termElement = document.getElementById("searchTerm");
    getFlamegraph().search(termElement.value);
  });

  console.log("popstate listener");
  window.addEventListener("popstate", handleFragments);
  console.log("resize listener");
  window.addEventListener("resize", debounced(onResize));

  // Enable tooltips
  console.log("enable tooltips");
  $('[data-toggle-second="tooltip"]').tooltip();
  $('[data-toggle="tooltip"]').tooltip();

  // Set up the reload handler
  console.log("setup reload handler");
  document
    .getElementById("plot")
    .on("plotly_relayout", refreshFlamegraphDebounced);

  // Set up initial data
  console.log("setup initial data");
  refreshFlamegraphDebounced({});

  // Enable toasts
  console.log("enable toasts");
  var toastElList = [].slice.call(document.querySelectorAll(".toast"));
  var toastList = toastElList.map(function (toastEl) {
    return new bootstrap.Toast(toastEl, { delay: 10000 });
  });
  toastList.forEach((toast) => toast.show()); // This show them
}

document.addEventListener("DOMContentLoaded", main);
