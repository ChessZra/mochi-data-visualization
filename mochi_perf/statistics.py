"""
Preliminary code from 
https://github.com/mochi-hpc/mochi-performance-analysis/

Refactored to multiprocessing 08/08/2025

Unimplemented dataframes:
    bulk_create_ddf, bulk_transfer_ddf, progress_loop_ddf
"""

import multiprocessing as mp
import orjson
import pandas as pd
import os
import time
from collections import defaultdict
from typing import List, Dict, Any, Optional


class MochiStatistics:

    def __init__(self, files: List[str] = [], num_cores: int = None):
        self.num_cores = num_cores or mp.cpu_count()
        self.origin_rpc_df = None
        self.target_rpc_df = None
        # Unused as of now- could be useful for future dashboard features
        self.bulk_create_df = None
        self.bulk_transfer_df = None
        self.progress_loop_df = None
        if files:
            start = time.time()
            self.add_files_multiprocessing(files)
            print(f'Parsed {len(files)} files in {time.time() - start:.2f} seconds')

    def clear(self):
        self.origin_rpc_df = None
        self.target_rpc_df = None
        self.bulk_create_df = None
        self.bulk_transfer_df = None
        self.progress_loop_df = None

    def _read_file_content(self, filename: str):
        """Read a single file and return its content"""
        try:
            with open(filename, 'rb') as f:
                return filename, f.read()
        except Exception as e:
            print(f"Error reading file {filename}: {str(e)}")
            return filename, None
   
    def _parse_file_batch(self, file_contents_batch):
        """Parse a batch of file contents into DataFrames for both origin and target"""
        origin_results = []
        target_results = []
        for filename, file_content in file_contents_batch:
            if file_content is None:
                continue
            try:
                content = orjson.loads(file_content)
                rpcs = content['rpcs']
                address = content['address']
                basename = os.path.basename(filename)
                # Check if address field is empty
                if not address or address == '':
                    continue
                # Try to parse as origin file
                origin_rpcs = {k:v for k, v in rpcs.items() if 'origin' in v}
                if len(origin_rpcs) > 0:
                    result = self._make_rpc_stats_df(basename, address, rpcs, 'origin', 'sent_to')
                    if result is not None and len(result) > 0:
                        origin_results.append(result)
                # Try to parse as target file
                target_rpcs = {k:v for k, v in rpcs.items() if 'target' in v}
                if len(target_rpcs) > 0:
                    # Check if it's actually a target file (not just bulk operations)
                    is_target_file = False
                    for rpc in target_rpcs.values():
                        section = rpc['target']
                        for peer_key, operations in section.items():
                            operations = {k: v for k, v in operations.items() if k != 'bulk'}
                            if len(operations) > 0: 
                                is_target_file = True
                                break
                        if is_target_file:
                            break
                    if is_target_file:
                        result = self._make_rpc_stats_df(basename, address, rpcs, 'target', 'received_from')
                        if result is not None and len(result) > 0:
                            target_results.append(result)
            except Exception as e:
                print(f"Error parsing file {filename}: {str(e)}")
                continue
        # Combine results for this batch
        origin_df = pd.concat(origin_results, axis=0, ignore_index=False) if origin_results else None
        target_df = pd.concat(target_results, axis=0, ignore_index=False) if target_results else None
        return origin_df, target_df
    
    def add_files_multiprocessing(self, files: List[str]):
        """Add files using multiprocessing approach as suggested by Ankush"""
        print(f"Processing {len(files)} files using {self.num_cores} cores...")
        # Step 1: Read all files in parallel (lots of threads)
        file_read_start = time.time()
        print("Reading files in parallel (multiprocessing)...")
        with mp.Pool(min(32, len(files))) as read_pool:
            file_contents = read_pool.map(self._read_file_content, files)
        # Filter out failed reads
        valid_file_contents = [(filename, content) for filename, content in file_contents if content is not None]
        file_read_time = time.time() - file_read_start
        print(f"File reading took {file_read_time:.2f} seconds for {len(valid_file_contents)} files")
        # Step 2: Parse files in parallel (one thread per core)
        parse_start = time.time()
        print("Parsing files in parallel...")
        # Split file contents into batches for parsing
        batch_size = max(1, len(valid_file_contents) // self.num_cores)
        file_batches = [valid_file_contents[i:i+batch_size] for i in range(0, len(valid_file_contents), batch_size)]
        with mp.Pool(min(8, self.num_cores)) as parse_pool:  # Limit parsing processes to avoid Windows handle limit
            batch_results = parse_pool.map(self._parse_file_batch, file_batches)
        parse_time = time.time() - parse_start
        print(f"File parsing took {parse_time:.2f} seconds")
        # Step 3: Combine all results
        combine_start = time.time()
        print("Combining results...")
        origin_dfs = [result[0] for result in batch_results if result[0] is not None]
        target_dfs = [result[1] for result in batch_results if result[1] is not None]
        self.origin_rpc_df = pd.concat(origin_dfs, axis=0, ignore_index=False) if origin_dfs else pd.DataFrame()
        self.target_rpc_df = pd.concat(target_dfs, axis=0, ignore_index=False) if target_dfs else pd.DataFrame()
        combine_time = time.time() - combine_start
        print(f"Result combination took {combine_time:.2f} seconds")
        total_time = time.time() - file_read_start
        print(f"Total processing took {total_time:.2f} seconds")
        print(f"Found {len(self.origin_rpc_df)} origin records, {len(self.target_rpc_df)} target records") 

    """ Static methods used for multiprocessing """
    def _parse_file_content(self, filename: str, file_content: bytes, section_name: str):
        """Parse file content that's already been read from disk"""
        try:
            content = orjson.loads(file_content)
            rpcs = content['rpcs']
            progress_loop = content['progress_loop']
            address = content['address']
            basename = os.path.basename(filename)
            if section_name == 'origin':
                result = self._make_rpc_stats_df(basename, address, rpcs, 'origin', 'sent_to')
            elif section_name == 'target':
                result = self._make_rpc_stats_df(basename, address, rpcs, 'target', 'received_from')
            # Unused:
            elif section_name == 'bulk_create':
                result = self._make_bulk_create_stats_df(basename, address, rpcs)
            elif section_name == 'bulk_transfer':
                result = self._make_bulk_transfer_stats_df(basename, address, rpcs)
            elif section_name == 'progress_loop':
                result = self._make_progress_loop_stats_df(basename, address, progress_loop)
            else:
                raise Exception('Invalid option for _parse_file_content')            
            return result
        except Exception as e:
            print(f"Error processing file {filename}: {str(e)}")
            return None

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
        # Sort columns for better performance with MultiIndex access
        df = df.sort_index(axis=1)
        return df

    def _make_bulk_create_stats_df(self, filename: str, address: str, rpcs: dict):
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
        # Sort columns for better performance with MultiIndex access
        df = df.sort_index(axis=1)
        return df

    def _make_bulk_transfer_stats_df(self, filename: str, address: str, rpcs: dict):
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
        # Sort columns for better performance with MultiIndex access
        df = df.sort_index(axis=1)
        return df

    @staticmethod
    def _make_progress_loop_stats_df(filename: str, address: str, progress_loop: dict):
        columns = defaultdict(list)
        index = []
        # Loop through progress loop data
        for statsblock, stats in progress_loop.items():
            # stats columns
            for statskey, statsval in stats.items():
                columns[(statsblock, statskey, '')].append(statsval)
            index.append((filename, address, statsblock))
        index = pd.MultiIndex.from_tuples(index,
            names=['file', 'address', 'statsblock'])
        df = pd.DataFrame(columns, index=index)
        df.columns.name = ('operation', 'category', 'statistics')
        # Sort columns for better performance with MultiIndex access
        df = df.sort_index(axis=1)
        return df
