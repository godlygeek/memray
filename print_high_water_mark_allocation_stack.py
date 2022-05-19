import os
import sys

from memray import FileReader

with FileReader(os.fspath(sys.argv[1])) as reader:
    for i, record in enumerate(reader.get_allocation_records()):
        if i == reader.get_high_watermark_index():
            print(record.stack_trace())
            break
