#pragma once

#include <cerrno>
#include <climits>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <unistd.h>

#include "records.h"
#include "sink.h"

namespace memray::tracking_api {

class Transaction
{
  public:
    template<typename T>
    void inline writeSimpleType(T&& item);

    void inline writeString(const char* the_string);

    template<typename T>
    void inline writeRecord(const RecordType& token, const T& item);

  private:
    friend class RecordWriter;
    Transaction(std::vector<char>&& buffer);

    Transaction(Transaction& other) = delete;
    Transaction& operator=(const Transaction&) = delete;

    void appendBlob(const char* src, size_t size);

    int d_numAllocations{0};
    int d_numFrames{0};
    std::vector<char> d_buffer;
};

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

    bool writeHeader(bool seek_to_start);

    Transaction startTransaction();
    bool commitTransaction(Transaction&& transaction);

    std::unique_lock<std::mutex> acquireLock();
    std::unique_ptr<RecordWriter> cloneInChildProcess();

  private:
    // Data members
    int d_version{CURRENT_HEADER_VERSION};
    std::unique_ptr<memray::io::Sink> d_sink;
    std::mutex d_mutex;
    HeaderRecord d_header{};
    TrackerStats d_stats{};
    std::vector<std::vector<char>> d_transactionBuffers{};
};

void inline Transaction::appendBlob(const char* src, size_t size)
{
    d_buffer.insert(d_buffer.end(), src, src + size);
}

template<typename T>
void inline Transaction::writeSimpleType(T&& item)
{
    appendBlob(reinterpret_cast<const char*>(&item), sizeof(item));
}

void inline Transaction::writeString(const char* the_string)
{
    appendBlob(the_string, strlen(the_string) + 1);
}

template<typename T>
void inline Transaction::writeRecord(const RecordType& token, const T& item)
{
    static_assert(
            std::is_trivially_copyable<T>::value,
            "Called writeRecord on binary records which cannot be trivially copied");

    if (token == RecordType::ALLOCATION) {
        d_numAllocations += 1;
    }
    appendBlob(reinterpret_cast<const char*>(&token), sizeof(RecordType));
    appendBlob(reinterpret_cast<const char*>(&item), sizeof(T));
}

template<>
void inline Transaction::writeRecord(const RecordType& token, const pyrawframe_map_val_t& item)
{
    d_numFrames += 1;
    writeSimpleType(token);
    writeSimpleType(item.first);
    writeString(item.second.function_name);
    writeString(item.second.filename);
    writeSimpleType(item.second.lineno);
}

template<>
void inline Transaction::writeRecord(const RecordType& token, const SegmentHeader& item)
{
    writeSimpleType(token);
    writeString(item.filename);
    writeSimpleType(item.num_segments);
    writeSimpleType(item.addr);
}

template<>
void inline Transaction::writeRecord(const RecordType& token, const ThreadRecord& record)
{
    writeSimpleType(token);
    writeSimpleType(record.tid);
    writeString(record.name);
}

}  // namespace memray::tracking_api
