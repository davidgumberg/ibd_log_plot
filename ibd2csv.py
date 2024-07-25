import csv
import re
import sys
from datetime import datetime
from dataclasses import dataclass

def btc_date2str(string:str) -> datetime:
    return datetime.strptime(string, "%Y-%m-%dT%H:%M:%SZ")

@dataclass
class BlockData:
    # Timestamp of the first message
    start_timestamp: datetime = datetime.now()

    # Timestamp of the final message
    end_timestamp: datetime = datetime.now()

    # Block metadata provided by 'UpdateTip' messages
    # time of the block
    date: datetime = datetime.now()
    height: int = 0
    log2_work: float = 0.0
    tx_total: int = 0
    progress: float = 0.0
    # On-disk size of the dbcache at the time of this block
    cache_size: float = 0.0
    # number of tx's in dbcache at the time of this block
    cache_count: int = 0

    # time to load the block from disk
    disk_load_time: float = 0.0

    sanity_check_time: float = 0.0

    fork_check_time: float = 0.0

    # number of tx's connected during block connection
    tx_connect_count: int = 0
    tx_connect_time: float = 0.0

    # block_tx_connect_time_per_tx: float = 0.0

    # number of tx inputs verified while connecting block
    txin_count: int = 0
    txin_verify_time: float = 0.0

    # txin_verify_time_per_txin:  float = 0.0

    write_undo_time: float = 0.0

    write_index_time: float = 0.0

    # Confusingly named, is the amount of time it takes to do everything
    # above, so it's the amount of time it takes to connect a block minus
    # the postprocessing time, should really be called *subtotal*
    connect_total_time: float = 0.0
    
    flush_time: float = 0.0

    write_chainstate_time: float = 0.0

    # block connection postprocessing time
    postprocess_time: float = 0.0

    # Confusingly named, is the real *grand* total of time to connect the
    # block, including postprocess
    connect_block_time: float = 0.0

def write_block_to_csv(writer, block):
    writer.writerow([
        block.start_timestamp, # column 1
        block.end_timestamp, # column 2
        block.date, # column 3
        block.height, # column 4
        block.log2_work, # column 5
        block.tx_total, # column 6
        block.progress, # column 7
        block.cache_size, # column 8
        block.cache_count, # column 9
        block.disk_load_time, # column 10
        block.sanity_check_time, # column 11
        block.fork_check_time, # column 12
        block.tx_connect_count, # column 13
        block.tx_connect_time, # column 14
        block.txin_count, # column 15
        block.txin_verify_time, # column 16
        block.write_undo_time, # column 17
        block.write_index_time, # column 18
        block.connect_total_time, # column 19
        block.flush_time, # column 20
        block.write_chainstate_time, # column 21
        block.postprocess_time, # column 22
        block.connect_block_time # column 23
    ])


timestamp_pattern = r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z'
patterns : dict = {
    'bench': re.compile(timestamp_pattern + r' \[bench\]'),

    # UpdateTip Pattern
    # 2024-07-19T06:20:15Z UpdateTip: new best=000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f height=0 version=0x00000001 log2_work=32.000022 tx=1 date='2009-01-03T18:15:05Z' progress=0.000000 cache=0.3MiB(0txo)
    'updatetip': re.compile(
        timestamp_pattern +
        r' UpdateTip: new best=(?P<hash>[0-9a-f]{64})' +
        r' height=(?P<height>\d+)' +
        r' version=(?P<version>0x[0-9a-f]+)' +
        r' log2_work=(?P<log2_work>\d+\.\d+)' +
        r' tx=(?P<tx_total>\d+)' +
        r" date='(?P<date>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)'" +
        r' progress=(?P<progress>\d+\.\d+)' +
        r' cache=(?P<cache_size>\d+\.\d+)MiB\((?P<cache_count>\d+)txo\)'
    ),

    # BlockData::disk_load_time pattern
    # 2024-07-19T06:25:30Z [bench]   - Load block from disk: 0.01ms
    'disk_load': re.compile(
        r'   - Load block from disk: ' +
        r'(?P<time>\d+\.\d+)ms'
    ),

    # BlockData::sanity_check_time pattern
    # 2024-07-19T06:25:30Z [bench]     - Sanity checks: 0.01ms [1.34s (0.01ms/blk)]
    'sanity_check': re.compile(
        r'     - Sanity checks: ' +
        r'(?P<time>\d+\.\d+)ms ' +
        r'\[(?P<total_time>\d+\.\d+)s ' +
        r'\((?P<avg_time>\d+\.\d+)ms/blk\)\]'
    ),

    # BlockData::fork_check_time pattern
    # 2024-07-19T06:25:30Z [bench]     - Fork checks: 1.12ms [58.31s (0.29ms/blk)]
    'fork_check': re.compile(
        r'     - Fork checks: ' +
        r'(?P<time>\d+\.\d+)ms ' +
        r'\[(?P<total_time>\d+\.\d+)s ' +
        r'\((?P<avg_time>\d+\.\d+)ms/blk\)\]'
    ),

    # BlockData::tx_connect* pattern
    # 2024-07-19T06:25:30Z [bench]       - Connect 111 transactions: 2.10ms (0.019ms/tx, 0.003ms/txin) [34.25s (0.17ms/blk)]
    'tx_connect': re.compile(
        r'       - Connect (?P<count>\d+) transactions: ' +
        r'(?P<time>\d+\.\d+)ms ' +
        r'\((?P<time_per_count>\d+\.\d+)ms/tx, \d+.\d+ms/txin\) ' +
        r'\[(?P<total_time>\d+\.\d+)s ' +
        r'\((?P<avg_time>\d+\.\d+)ms/blk\)\]'
    ),

    # BlockData::txin* pattern
    # 2024-07-19T06:25:30Z [bench]     - Verify 647 txins: 2.12ms (0.003ms/txin) [36.44s (0.18ms/blk)]
    'txin': re.compile(
        r'     - Verify (?P<count>\d+) txins: ' +
        r'(?P<time>\d+\.\d+)ms ' +
        r'\((?P<time_per_count>\d+\.\d+)ms/txin\) ' +
        r'\[(?P<total_time>\d+\.\d+)s ' +
        r'\((?P<avg_time>\d+\.\d+)ms/blk\)\]'
    ),

    # BlockData::write_undo_time pattern
    # 2024-07-19T06:20:45Z [bench]     - Index writing: 0.01ms [0.00s (0.01ms/blk)]
    'write_undo': re.compile(
        r'     - Write undo data: ' +
        r'(?P<time>\d+\.\d+)ms ' +
        r'\[(?P<total_time>\d+\.\d+)s ' +
        r'\((?P<avg_time>\d+\.\d+)ms/blk\)\]'
    ),

    # BlockData::write_index_time pattern
    # 2024-07-19T06:20:45Z [bench]     - Index writing: 0.01ms [0.00s (0.01ms/blk)]
    'write_index': re.compile(
        r'     - Index writing: ' +
        r'(?P<time>\d+\.\d+)ms ' +
        r'\[(?P<total_time>\d+\.\d+)s ' +
        r'\((?P<avg_time>\d+\.\d+)ms/blk\)\]'
    ),

    # remember: connect_total_time is *not* the total time, it is the subtotal
    # without postprocessing, see the comment above.
    # BlockData::connect_total_time pattern
    # 2024-07-19T06:20:45Z [bench]   - Connect total: 0.09ms [0.01s (0.09ms/blk)]
    'connect_total': re.compile(
        r'   - Connect total: ' +
        r'(?P<time>\d+\.\d+)ms ' +
        r'\[(?P<total_time>\d+\.\d+)s ' +
        r'\((?P<avg_time>\d+\.\d+)ms/blk\)\]'
    ),

    # BlockData::flush_time pattern
    # 2024-07-19T06:20:45Z [bench]   - Flush: 0.01ms [0.00s (0.01ms/blk)]
    'flush': re.compile(
        r'   - Flush: ' +
        r'(?P<time>\d+\.\d+)ms ' +
        r'\[(?P<total_time>\d+\.\d+)s ' +
        r'\((?P<avg_time>\d+\.\d+)ms/blk\)\]'
    ),

    # BlockData::write_chainstate pattern
    # 2024-07-19T06:20:45Z [bench]   - Flush: 0.01ms [0.00s (0.01ms/blk)]
    'write_chainstate': re.compile(
        r'   - Writing chainstate: ' +
        r'(?P<time>\d+\.\d+)ms ' +
        r'\[(?P<total_time>\d+\.\d+)s ' +
        r'\((?P<avg_time>\d+\.\d+)ms/blk\)\]'
    ),

    # BlockData::postprocess_pattern
    # 2024-07-19T06:20:15Z [bench]   - Connect postprocess: 0.07ms [0.00s (0.07ms/blk)]
    'postprocess': re.compile(
        r'   - Connect postprocess: ' +  \
        r'(\d+\.\d+)ms \[(\d+\.\d+)s \((\d+\.\d+)ms/blk\)\]'
    ),

    # BlockData::connect_block_time
    # 2024-07-19T06:20:15Z [bench] - Connect block: 0.27ms [0.00s (0.27ms/blk)]
    'connect_block_time': re.compile(
        r' - Connect block: ' +  \
        r'(\d+\.\d+)ms \[(\d+\.\d+)s \((\d+\.\d+)ms/blk\)\]'
    )
}

def parse_debug_log(log_path, csv_path):
    with open(log_path, 'r') as log_file, open(csv_path, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        
        # Write header
        csv_writer.writerow([
            'start_timestamp', 'end_timestamp', 'date', 'height', 'log2_work',
            'tx_total', 'progress', 'cache_size', 'cache_count', 'disk_load_time',
            'sanity_check_time', 'fork_check_time', 'tx_connect_count',
            'tx_connect_time', 'txin_count', 'txin_verify_time', 'write_undo_time',
            'write_index_time', 'connect_total_time', 'flush_time',
            'write_chainstate_time', 'postprocess_time', 'connect_block_time'
        ])

        # Our working block
        w_block = BlockData()
        for line in log_file:
            # Parse timestamp
            timestamp_match = re.match(timestamp_pattern, line)
            if not timestamp_match:
                continue
            else:
                timestamp = btc_date2str(timestamp_match.group(0))

            bench_match = patterns['bench'].match(line)
            if bench_match:
                # the part after the end of the pattern
                line = line[bench_match.end():]

                match = patterns['disk_load'].search(line)
                if match:
                    w_block.disk_load_time = float(match.group('time'))
                    w_block.start_timestamp = timestamp
                    continue

                match = patterns['sanity_check'].search(line)
                if match:
                    w_block.sanity_check_time = float(match.group('time'))
                    continue

                match = patterns['fork_check'].search(line)
                if match:
                    w_block.fork_check_time = float(match.group('time'))
                    continue

                match = patterns['tx_connect'].search(line)
                if match:
                    w_block.tx_connect_count = int(match.group('count'))
                    w_block.tx_connect_time = float(match.group('time'))

                    continue

                match = patterns['txin'].search(line)
                if match:
                    w_block.txin_count = int(match.group('count'))
                    w_block.txin_verify_time = float(match.group('time'))
                    continue

                match = patterns['write_undo'].search(line)
                if match:
                    w_block.write_undo_time = float(match.group('time'))
                    continue

                match = patterns['write_index'].search(line)
                if match:
                    w_block.write_index_time = float(match.group('time'))
                    continue

                # remember, not the true total, this is the subtotal
                match = patterns['connect_total'].search(line)
                if match:
                    w_block.connect_total_time = float(match.group('time'))
                    continue

                match = patterns['flush'].search(line)
                if match:
                    w_block.flush_time = float(match.group('time'))
                    continue

                match = patterns['write_chainstate'].search(line)
                if match:
                    w_block.write_chainstate_time = float(match.group('time'))
                    continue

                match = patterns['postprocess'].search(line)
                if match:
                    w_block.postprocess_time = float(match.group(1))
                    continue

                # grand total, when we see this pattern, it's the last message
                # and we should flush w_block to disk and create a new one.
                match = patterns['connect_block_time'].search(line)
                if match:
                    w_block.connect_block_time = float(match.group(1))
                    w_block.end_timestamp = timestamp
                    write_block_to_csv(csv_writer, w_block)
                    w_block = BlockData()  # Create a new block
                    continue


                continue
            tip_match = patterns['updatetip'].match(line)
            if tip_match:
                # I think dict access is easier than calling group on the match
                tip = tip_match.groupdict()

                w_block.height = int(tip['height'])
                w_block.log2_work = float(tip['log2_work'])
                w_block.tx_total = int(tip['tx_total'])
                w_block.date = btc_date2str(tip['date'])
                w_block.progress = float(tip['progress'])
                w_block.cache_size = float(tip['cache_size'])
                w_block.cache_count = int(tip['cache_count'])

                continue

def main():
    if len(sys.argv) != 3:
        print("Usage: python ibd2csv.py <path_to_debug.log> <output.csv>")
        sys.exit(1)
    
    debug_log_path = sys.argv[1]
    output_csv_path = sys.argv[2]

    try:
        parse_debug_log(debug_log_path, output_csv_path)
    except FileNotFoundError:
        print(f"Error: File not found - {debug_log_path}")
        sys.exit(1)
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
