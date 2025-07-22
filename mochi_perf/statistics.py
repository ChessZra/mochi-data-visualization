"""
Preliminary code from 
https://github.com/mochi-hpc/mochi-performance-analysis/
"""

import json
import pandas as pd
import os
import time
import psutil
from collections import defaultdict
from typing import List, Dict, Any
from contextlib import contextmanager


class ProfilerStats:
    """Container for profiling statistics"""
    
    def __init__(self):
        self.reset()
    
    def reset(self):
        self.total_time = 0.0
        self.file_processing_times = []
        self.method_times = defaultdict(list)
        self.memory_usage = []
        self.files_processed = 0
        self.files_skipped = 0
        self.total_records = {
            'origin_rpc': 0,
            'target_rpc': 0,
            'bulk_create': 0,
            'bulk_transfer': 0,
            'progress_loop': 0
        }
        self.errors = []
    
    def add_method_time(self, method_name: str, duration: float):
        self.method_times[method_name].append(duration)
    
    def add_memory_sample(self, memory_mb: float):
        self.memory_usage.append(memory_mb)
    
    def add_error(self, error_msg: str):
        self.errors.append(error_msg)
    
    def get_summary(self) -> Dict[str, Any]:
        """Generate a summary of profiling statistics"""
        summary = {
            'total_time_seconds': self.total_time,
            'files_processed': self.files_processed,
            'files_skipped': self.files_skipped,
            'total_records': self.total_records.copy(),
            'errors': len(self.errors),
            'memory_stats': {},
            'method_performance': {}
        }
        
        if self.memory_usage:
            summary['memory_stats'] = {
                'peak_mb': max(self.memory_usage),
                'average_mb': sum(self.memory_usage) / len(self.memory_usage),
                'samples': len(self.memory_usage)
            }
        
        for method, times in self.method_times.items():
            if times:
                summary['method_performance'][method] = {
                    'total_time': sum(times),
                    'average_time': sum(times) / len(times),
                    'calls': len(times),
                    'min_time': min(times),
                    'max_time': max(times)
                }
        
        if self.file_processing_times:
            summary['file_processing'] = {
                'average_time_per_file': sum(self.file_processing_times) / len(self.file_processing_times),
                'fastest_file': min(self.file_processing_times),
                'slowest_file': max(self.file_processing_times)
            }
        
        return summary


class MochiStatistics:

    def __init__(self, files: List[str] = [], enable_profiling: bool = False):
        self.origin_rpc_df = pd.DataFrame()
        self.target_rpc_df = pd.DataFrame()
        self.bulk_create_df = pd.DataFrame()
        self.bulk_transfer_df = pd.DataFrame()
        self.progress_loop_df = pd.DataFrame()
        
        # Profiling setup
        self.enable_profiling = enable_profiling
        self.profiler_stats = ProfilerStats()
        self._process_start_time = None
        
        if enable_profiling:
            print("Profiling enabled for MochiStatistics")
        
        for filename in files:
            self.add_file(filename)

    @contextmanager
    def _profile_method(self, method_name: str):
        """Context manager for profiling individual methods"""
        if not self.enable_profiling:
            yield
            return
        
        start_time = time.perf_counter()
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        try:
            yield
        finally:
            end_time = time.perf_counter()
            end_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
            
            duration = end_time - start_time
            self.profiler_stats.add_method_time(method_name, duration)
            self.profiler_stats.add_memory_sample(end_memory)

    def clear(self):
        with self._profile_method('clear'):
            self.origin_rpc_df = pd.DataFrame()
            self.target_rpc_df = pd.DataFrame()
            self.bulk_create_df = pd.DataFrame()
            self.bulk_transfer_df = pd.DataFrame()
            self.progress_loop_df = pd.DataFrame()
            
            if self.enable_profiling:
                self.profiler_stats.reset()

    def add_file(self, filename: str):
        with self._profile_method('add_file'):
            file_start_time = time.perf_counter()
            
            try:
                # Check if file is empty before trying to parse JSON
                if os.path.getsize(filename) == 0:
                    print(f"Warning: Skipping empty file {filename}")
                    if self.enable_profiling:
                        self.profiler_stats.files_skipped += 1
                    return
                    
                with open(filename, 'r') as f:
                    content = json.load(f)
                rpcs = content['rpcs']
                
                progress_loop = content['progress_loop']
                address = content['address']
                basename = os.path.basename(filename)

                if not address or address == '':
                    address = f"unknown-{basename.split('.')[0]}"

                # Track record counts before processing
                initial_counts = {
                    'origin_rpc': len(self.origin_rpc_df),
                    'target_rpc': len(self.target_rpc_df),
                    'bulk_create': len(self.bulk_create_df),
                    'bulk_transfer': len(self.bulk_transfer_df),
                    'progress_loop': len(self.progress_loop_df)
                }

                self._add_origin_rpc_stats(basename, address, rpcs)
                self._add_target_rpc_stats(basename, address, rpcs)
                self._add_bulk_create_stats(basename, address, rpcs)
                self._add_bulk_transfer_stats(basename, address, rpcs)
                self._add_progress_loop_stats(basename, address, progress_loop)
                
                if self.enable_profiling:
                    # Update record counts
                    self.profiler_stats.total_records['origin_rpc'] += len(self.origin_rpc_df) - initial_counts['origin_rpc']
                    self.profiler_stats.total_records['target_rpc'] += len(self.target_rpc_df) - initial_counts['target_rpc']
                    self.profiler_stats.total_records['bulk_create'] += len(self.bulk_create_df) - initial_counts['bulk_create']
                    self.profiler_stats.total_records['bulk_transfer'] += len(self.bulk_transfer_df) - initial_counts['bulk_transfer']
                    self.profiler_stats.total_records['progress_loop'] += len(self.progress_loop_df) - initial_counts['progress_loop']
                    
                    self.profiler_stats.files_processed += 1
                    file_duration = time.perf_counter() - file_start_time
                    self.profiler_stats.file_processing_times.append(file_duration)
                    
            except Exception as e:
                error_msg = f"Error processing file {filename}: {str(e)}"
                print(f"Error: {error_msg}")
                if self.enable_profiling:
                    self.profiler_stats.add_error(error_msg)

    def _get_rpc_info(self, rpc: dict):
        return (rpc['name'], rpc['rpc_id'], rpc['provider_id'],
                rpc['parent_rpc_id'], rpc['parent_provider_id'])

    def _make_rpc_stats_df(self, filename: str, address: str, rpcs: dict, section_name: str, peer_index: str):
        rpcs = {k:v for k, v in rpcs.items() if section_name in v}
        columns = defaultdict(list)
        index = []
        for rpc in rpcs.values():
            name, rpc_id, provider_id, parent_rpc_id, parent_provider_id = self._get_rpc_info(rpc)
            section = rpc[section_name]
            for peer_key, operations in section.items():
                peer_address = peer_key.split()[-1]
                operations = {k: v for k, v in operations.items() if k != 'bulk'}
                if len(operations) == 0: continue
                for operation, statsblock in operations.items():
                    for statsname, stats in statsblock.items():
                        for statstype, statsval in stats.items():
                            columns[(operation, statsname, statstype)].append(statsval)
                index.append((filename, address, name, rpc_id, provider_id,
                              parent_rpc_id, parent_provider_id, peer_address))
        index = pd.MultiIndex.from_tuples(index,
            names=['file', 'address', 'name', 'rpc_id', 'provider_id',
                   'parent_rpc_id', 'parent_provider_id', peer_index])
        df = pd.DataFrame(columns, index=index)
        df.columns.name = ('operation', 'category', 'statistics')
        return df

    def _add_origin_rpc_stats(self, filename: str, address: str, rpcs: dict):
        with self._profile_method('_add_origin_rpc_stats'):
            df = self._make_rpc_stats_df(filename, address, rpcs, 'origin', 'sent_to')
            self.origin_rpc_df = pd.concat([self.origin_rpc_df, df])

    def _add_target_rpc_stats(self, filename: str, address: str, rpcs: dict):
        with self._profile_method('_add_target_rpc_stats'):
            df = self._make_rpc_stats_df(filename, address, rpcs, 'target', 'received_from')
            self.target_rpc_df = pd.concat([self.target_rpc_df, df])

    def _add_bulk_create_stats(self, filename: str, address: str, rpcs: dict):
        with self._profile_method('_add_bulk_create_stats'):
            rpcs = {k:v for k, v in rpcs.items() if 'target' in v}
            columns = defaultdict(list)
            index = []
            for rpc in rpcs.values():
                name, rpc_id, provider_id, parent_rpc_id, parent_provider_id = self._get_rpc_info(rpc)
                target = rpc['target']
                for received_from, operations in target.items():
                    if 'bulk' not in operations: continue
                    if 'create' not in operations['bulk']: continue
                    create = operations['bulk']['create']
                    received_from_address = received_from.split()[-1]
                    for statsname, stats in create.items():
                        for statstype, statsval in stats.items():
                            columns[('bulk_create', statsname, statstype)].append(statsval)
                    index.append((filename, address, name, rpc_id, provider_id,
                                  parent_rpc_id, parent_provider_id, received_from_address))
            index = pd.MultiIndex.from_tuples(index,
                names=['file', 'address', 'name', 'rpc_id', 'provider_id',
                       'parent_rpc_id', 'parent_provider_id', 'received_from'])
            df = pd.DataFrame(columns, index=index)
            df.columns.name = ('operation', 'category', 'statistics')
            self.bulk_create_df = pd.concat([self.bulk_create_df, df])

    def _add_bulk_transfer_stats(self, filename: str, address: str, rpcs: dict):
        with self._profile_method('_add_bulk_transfer_stats'):
            rpcs = {k:v for k, v in rpcs.items() if 'target' in v}
            columns = defaultdict(list)
            index = []
            for rpc in rpcs.values():
                name, rpc_id, provider_id, parent_rpc_id, parent_provider_id = self._get_rpc_info(rpc)
                target = rpc['target']
                for received_from, operations in target.items():
                    if 'bulk' not in operations: continue
                    bulk = operations['bulk']
                    if 'create' in bulk:
                        del bulk['create']
                    if len(bulk) == 0: continue
                    received_from_address = received_from.split()[-1]
                    for transfer_key, transfer_stats in bulk.items():
                        transfer_type, _, peer_address = transfer_key.split()
                        for bulk_operation, statsblock in transfer_stats.items():
                            for statsname, stats in statsblock.items():
                                for statstype, statsval in stats.items():
                                    columns[(bulk_operation, statsname, statstype)].append(statsval)
                        index.append((filename, address, name, rpc_id, provider_id,
                                      parent_rpc_id, parent_provider_id, received_from_address,
                                      transfer_type, peer_address))
            index = pd.MultiIndex.from_tuples(index,
                names=['file', 'address', 'name', 'rpc_id', 'provider_id',
                       'parent_rpc_id', 'parent_provider_id', 'received_from',
                       'transfer_type', 'remote_address'])
            df = pd.DataFrame(columns, index=index)
            df.columns.name = ('operation', 'category', 'statistics')
            self.bulk_transfer_df = pd.concat([self.bulk_transfer_df, df])

    def _add_progress_loop_stats(self, filename: str, address: str, progress_loop: dict):
        with self._profile_method('_add_progress_loop_stats'):
            columns = defaultdict(list)
            for statsblock, stats in progress_loop.items():
                for statskey, statsval in stats.items():
                    columns[(statsblock, statskey)].append(statsval)
            index = pd.MultiIndex.from_tuples([(filename, address)], names=['file', 'address'])
            df = pd.DataFrame(columns, index=index)
            df.columns.name = ('category', 'statistics')
            self.progress_loop_df = pd.concat([self.progress_loop_df, df])

    def get_profiling_report(self) -> str:
        """Generate a formatted profiling report"""
        if not self.enable_profiling:
            return "Profiling is not enabled. Set enable_profiling=True when creating MochiStatistics instance."
        
        summary = self.profiler_stats.get_summary()
        
        report = []
        report.append("=" * 60)
        report.append("MOCHI STATISTICS PROFILING REPORT")
        report.append("=" * 60)
        
        report.append(f"Total Processing Time: {summary['total_time_seconds']:.3f} seconds")
        report.append(f"Files Processed: {summary['files_processed']}")
        report.append(f"Files Skipped: {summary['files_skipped']}")
        report.append(f"Errors Encountered: {summary['errors']}")
        
        report.append("\nRecords Processed:")
        for record_type, count in summary['total_records'].items():
            report.append(f"  {record_type}: {count:,}")
        
        if 'memory_stats' in summary and summary['memory_stats']:
            mem = summary['memory_stats']
            report.append(f"\nMemory Usage:")
            report.append(f"  Peak: {mem['peak_mb']:.1f} MB")
            report.append(f"  Average: {mem['average_mb']:.1f} MB")
        
        if 'file_processing' in summary:
            fp = summary['file_processing']
            report.append(f"\nFile Processing Performance:")
            report.append(f"  Average time per file: {fp['average_time_per_file']:.4f} seconds")
            report.append(f"  Fastest file: {fp['fastest_file']:.4f} seconds")
            report.append(f"  Slowest file: {fp['slowest_file']:.4f} seconds")
        
        if summary['method_performance']:
            report.append(f"\nMethod Performance:")
            for method, stats in summary['method_performance'].items():
                report.append(f"  {method}:")
                report.append(f"    Total time: {stats['total_time']:.4f}s")
                report.append(f"    Calls: {stats['calls']}")
                report.append(f"    Avg per call: {stats['average_time']:.4f}s")
        
        report.append("=" * 60)
        
        return "\n".join(report)
