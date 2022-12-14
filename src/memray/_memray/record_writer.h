#pragma once

#include <cerrno>
#include <climits>
#include <cstring>
#include <memory>
#include <string>
#include <type_traits>
#include <unistd.h>

#include "records.h"
#include "sink.h"

namespace memray::tracking_api {

class RecordWriter
{
  public:
    explicit RecordWriter(
            std::unique_ptr<memray::io::Sink> sink,
            const std::string& command_line,
            bool native_traces);

    RecordWriter(RecordWriter& other) = delete;
    RecordWriter(RecordWriter&& other) = delete;
    void operator=(const RecordWriter&) = delete;
    void operator=(RecordWriter&&) = delete;

    bool inline writeRecord(const MemoryRecord& record);
    bool inline writeRecord(const pyrawframe_map_val_t& item);
    bool inline writeRecord(const UnresolvedNativeFrame& record);

    bool inline writeMappings(const std::vector<ImageSegments>& mappings);

    bool inline writeThreadSpecificRecord(thread_id_t tid, const FramePop& record);
    bool inline writeThreadSpecificRecord(thread_id_t tid, const FramePush& record);
    bool inline writeThreadSpecificRecord(thread_id_t tid, const AllocationRecord& record);
    bool inline writeThreadSpecificRecord(thread_id_t tid, const NativeAllocationRecord& record);
    bool inline writeThreadSpecificRecord(thread_id_t tid, const ThreadRecord& record);

    bool writeHeader(bool seek_to_start);
    bool writeTrailer();

    void setMainTidAndSkippedFrames(thread_id_t main_tid, size_t skipped_frames_on_main_tid);
    std::unique_ptr<RecordWriter> cloneInChildProcess();

  private:
    bool inline maybeWriteContextSwitchRecordUnsafe(thread_id_t tid);

    template<typename T>
    bool inline writeSimpleType(const T& item);
    bool inline writeString(const char* the_string);
    bool inline writeVarint(size_t val);
    bool inline writeSignedVarint(ssize_t val);
    template<typename T>
    bool inline writeIntegralDelta(T* prev, T new_val);

    // Data members
    int d_version{CURRENT_HEADER_VERSION};
    std::unique_ptr<memray::io::Sink> d_sink;
    HeaderRecord d_header{};
    TrackerStats d_stats{};
    DeltaEncodedFields d_last;
};

template<typename T>
bool inline RecordWriter::writeSimpleType(const T& item)
{
    static_assert(
            std::is_trivially_copyable<T>::value,
            "writeSimpleType called on non trivially copyable type");

    return d_sink->writeAll(reinterpret_cast<const char*>(&item), sizeof(item));
};

bool inline RecordWriter::writeString(const char* the_string)
{
    return d_sink->writeAll(the_string, strlen(the_string) + 1);
}

bool inline RecordWriter::writeVarint(size_t rest)
{
    unsigned char next_7_bits = rest & 0x7f;
    rest >>= 7;
    while (rest) {
        next_7_bits |= 0x80;
        if (!writeSimpleType(next_7_bits)) {
            return false;
        }
        next_7_bits = rest & 0x7f;
        rest >>= 7;
    }

    return writeSimpleType(next_7_bits);
}

bool inline RecordWriter::writeSignedVarint(ssize_t val)
{
    // protobuf style "zig-zag" encoding
    // https://developers.google.com/protocol-buffers/docs/encoding#signed-ints
    // This encodes -64 through 63 in 1 byte, -8192 through 8191 in 2 bytes, etc
    size_t zigzag_val = (static_cast<size_t>(val) << 1)
                        ^ static_cast<size_t>(val >> std::numeric_limits<ssize_t>::digits);
    return writeVarint(zigzag_val);
}

template<typename T>
bool inline RecordWriter::writeIntegralDelta(T* prev, T new_val)
{
    ssize_t delta = new_val - *prev;
    *prev = new_val;
    return writeSignedVarint(delta);
}

bool inline RecordWriter::writeRecord(const MemoryRecord& record)
{
    RecordTypeAndFlags token{RecordType::MEMORY_RECORD, 0};
    return writeSimpleType(token) && writeVarint(record.rss)
           && writeVarint(record.ms_since_epoch - d_stats.start_time) && d_sink->flush();
}

bool inline RecordWriter::writeRecord(const pyrawframe_map_val_t& item)
{
    d_stats.n_frames += 1;
    RecordTypeAndFlags token{RecordType::FRAME_INDEX, !item.second.is_entry_frame};
    return writeSimpleType(token) && writeIntegralDelta(&d_last.python_frame_id, item.first)
           && writeString(item.second.function_name) && writeString(item.second.filename)
           && writeIntegralDelta(&d_last.python_line_number, item.second.lineno);
}

bool inline RecordWriter::writeRecord(const UnresolvedNativeFrame& record)
{
    return writeSimpleType(RecordTypeAndFlags{RecordType::NATIVE_TRACE_INDEX, 0})
           && writeIntegralDelta(&d_last.instruction_pointer, record.ip)
           && writeIntegralDelta(&d_last.native_frame_id, record.index);
}

bool inline RecordWriter::writeMappings(const std::vector<ImageSegments>& mappings)
{
    RecordTypeAndFlags start_token{RecordType::MEMORY_MAP_START, 0};
    if (!writeSimpleType(start_token)) {
        return false;
    }

    for (const auto& image : mappings) {
        RecordTypeAndFlags segment_header_token{RecordType::SEGMENT_HEADER, 0};
        if (!writeSimpleType(segment_header_token) || !writeString(image.filename.c_str())
            || !writeVarint(image.segments.size()) || !writeSimpleType(image.addr))
        {
            return false;
        }

        RecordTypeAndFlags segment_token{RecordType::SEGMENT, 0};

        for (const auto& segment : image.segments) {
            if (!writeSimpleType(segment_token) || !writeSimpleType(segment.vaddr)
                || !writeVarint(segment.memsz))
            {
                return false;
            }
        }
    }

    return true;
}

bool inline RecordWriter::maybeWriteContextSwitchRecordUnsafe(thread_id_t tid)
{
    if (d_last.thread_id == tid) {
        return true;  // nothing to do.
    }
    d_last.thread_id = tid;

    RecordTypeAndFlags token{RecordType::CONTEXT_SWITCH, 0};
    ContextSwitch record{tid};
    return writeSimpleType(token) && writeSimpleType(record);
}

bool inline RecordWriter::writeThreadSpecificRecord(thread_id_t tid, const FramePop& record)
{
    if (!maybeWriteContextSwitchRecordUnsafe(tid)) {
        return false;
    }

    size_t count = record.count;
    while (count) {
        uint8_t to_pop = (count > 16 ? 16 : count);
        count -= to_pop;

        to_pop -= 1;  // i.e. 0 means pop 1 frame, 15 means pop 16 frames
        RecordTypeAndFlags token{RecordType::FRAME_POP, to_pop};
        assert(token.flags == to_pop);
        if (!writeSimpleType(token)) {
            return false;
        }
    }

    return true;
}

bool inline RecordWriter::writeThreadSpecificRecord(thread_id_t tid, const FramePush& record)
{
    if (!maybeWriteContextSwitchRecordUnsafe(tid)) {
        return false;
    }

    RecordTypeAndFlags token{RecordType::FRAME_PUSH, 0};
    return writeSimpleType(token) && writeIntegralDelta(&d_last.python_frame_id, record.frame_id);
}

bool inline RecordWriter::writeThreadSpecificRecord(thread_id_t tid, const AllocationRecord& record)
{
    if (!maybeWriteContextSwitchRecordUnsafe(tid)) {
        return false;
    }

    d_stats.n_allocations += 1;
    RecordTypeAndFlags token{RecordType::ALLOCATION, static_cast<unsigned char>(record.allocator)};
    return writeSimpleType(token) && writeIntegralDelta(&d_last.data_pointer, record.address)
           && (hooks::allocatorKind(record.allocator) == hooks::AllocatorKind::SIMPLE_DEALLOCATOR
               || writeVarint(record.size));
}

bool inline RecordWriter::writeThreadSpecificRecord(
        thread_id_t tid,
        const NativeAllocationRecord& record)
{
    if (!maybeWriteContextSwitchRecordUnsafe(tid)) {
        return false;
    }

    d_stats.n_allocations += 1;
    RecordTypeAndFlags token{
            RecordType::ALLOCATION_WITH_NATIVE,
            static_cast<unsigned char>(record.allocator)};
    return writeSimpleType(token) && writeIntegralDelta(&d_last.data_pointer, record.address)
           && writeVarint(record.size)
           && writeIntegralDelta(&d_last.native_frame_id, record.native_frame_id);
}

bool inline RecordWriter::writeThreadSpecificRecord(thread_id_t tid, const ThreadRecord& record)
{
    if (!maybeWriteContextSwitchRecordUnsafe(tid)) {
        return false;
    }

    RecordTypeAndFlags token{RecordType::THREAD_RECORD, 0};
    return writeSimpleType(token) && writeString(record.name);
}

}  // namespace memray::tracking_api
