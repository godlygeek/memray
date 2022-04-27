#include <chrono>
#include <fcntl.h>
#include <stdexcept>

#include "record_writer.h"

namespace memray::tracking_api {

using namespace std::chrono;

static PythonAllocatorType
getPythonAllocator()
{
#if PY_VERSION_HEX >= 0x03080000
    const char* name = _PyMem_GetCurrentAllocatorName();
#elif PY_VERSION_HEX >= 0x03070000
    const char* name = _PyMem_GetAllocatorsName();
#else
    const char* name = "";
#endif
    std::string allocator_name = name != NULL ? name : "";
    if (allocator_name == "pymalloc") {
        return PythonAllocatorType::PYTHONALLOCATOR_PYMALLOC;
    }
    if (allocator_name == "pymalloc_debug") {
        return PythonAllocatorType::PYTHONALLOCATOR_PYMALLOC_DEBUG;
    }
    if (allocator_name == "malloc") {
        return PythonAllocatorType::PYTHONALLOCATOR_MALLOC;
    }
    return PythonAllocatorType::PYTHONALLOCATOR_OTHER;
}

Transaction::Transaction(std::vector<char>&& buffer)
: d_buffer(buffer)
{
}

RecordWriter::RecordWriter(
        std::unique_ptr<memray::io::Sink> sink,
        const std::string& command_line,
        bool native_traces)
: d_sink(std::move(sink))
, d_stats({0, 0, duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()})
{
    d_header = HeaderRecord{
            "",
            d_version,
            native_traces,
            d_stats,
            command_line,
            ::getpid(),
            getPythonAllocator()};
    strncpy(d_header.magic, MAGIC, sizeof(d_header.magic));
}

bool
RecordWriter::writeHeader(bool seek_to_start)
{
    std::lock_guard<std::mutex> lock(d_mutex);
    if (seek_to_start) {
        // If we can't seek to the beginning to the stream (e.g. dealing with a socket), just give
        // up.
        if (!d_sink->seek(0, SEEK_SET)) {
            return false;
        }
    }

    d_stats.end_time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    d_header.stats = d_stats;

    const HeaderRecord& h = d_header;
    return d_sink->writeAll(reinterpret_cast<const char*>(&h.magic), sizeof(h.magic))
           && d_sink->writeAll(reinterpret_cast<const char*>(&h.version), sizeof(h.version))
           && d_sink->writeAll(reinterpret_cast<const char*>(&h.native_traces), sizeof(h.native_traces))
           && d_sink->writeAll(reinterpret_cast<const char*>(&h.stats), sizeof(h.stats))
           && d_sink->writeAll(h.command_line.c_str(), strlen(h.command_line.c_str()) + 1)
           && d_sink->writeAll(reinterpret_cast<const char*>(&h.pid), sizeof(h.pid))
           && d_sink->writeAll(
                   reinterpret_cast<const char*>(&h.python_allocator),
                   sizeof(h.python_allocator));
}

Transaction
RecordWriter::startTransaction()
{
    std::lock_guard<std::mutex> lock(d_mutex);
    if (d_transactionBuffers.empty()) {
        d_transactionBuffers.emplace_back();
        d_transactionBuffers.back().reserve(1024);
    }
    std::vector<char> buf = std::move(d_transactionBuffers.back());
    d_transactionBuffers.pop_back();
    buf.clear();
    return Transaction(std::move(buf));
}

bool
RecordWriter::commitTransaction(Transaction&& transaction)
{
    std::lock_guard<std::mutex> lock(d_mutex);
    d_stats.n_allocations += transaction.d_numAllocations;
    d_stats.n_frames += transaction.d_numFrames;
    bool ret = d_sink->writeAll(&transaction.d_buffer[0], transaction.d_buffer.size());
    d_transactionBuffers.push_back(std::move(transaction.d_buffer));
    return ret;
}

std::unique_lock<std::mutex>
RecordWriter::acquireLock()
{
    return std::unique_lock<std::mutex>(d_mutex);
}

std::unique_ptr<RecordWriter>
RecordWriter::cloneInChildProcess()
{
    std::unique_ptr<io::Sink> new_sink = d_sink->cloneInChildProcess();
    if (!new_sink) {
        return {};
    }
    return std::make_unique<RecordWriter>(
            std::move(new_sink),
            d_header.command_line,
            d_header.native_traces);
}

}  // namespace memray::tracking_api
