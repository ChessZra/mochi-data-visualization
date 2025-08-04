"""
Preliminary code from 
https://github.com/mochi-hpc/mochi-performance-analysis/

Implemented dask 07/28/2025

Unimplemented dataframes:
    bulk_create_ddf, bulk_transfer_ddf, progress_loop_ddf
    Simply, modify the _validate_file function to ensure no column mismatch happens
    when merging dask dataframes.
"""

import dask
import orjson
import pandas as pd
import os
import time
import dask.dataframe as dd
from collections import defaultdict
from typing import List, Dict, Any
from math import ceil


class MochiStatistics:

    def __init__(self, files: List[str] = [], num_cores: int = 4):
        self.num_cores = num_cores
        self.origin_rpc_ddf = None
        self.target_rpc_ddf = None
        # Unused as of now- could be useful for future dashboard features
        self.bulk_create_ddf = None
        self.bulk_transfer_ddf = None
        self.progress_loop_ddf = None
        if files:            
            start = time.time()
            self.add_files_optimized(files)
            print(f'Parsed {len(files)} files in {time.time() - start} seconds')

    def clear(self):
        self.origin_rpc_ddf = None
        self.target_rpc_ddf = None
        self.bulk_create_ddf = None
        self.bulk_transfer_ddf = None
        self.progress_loop_ddf =  None
    
    def add_files_optimized(self, files: list[str]):
        # Validate files- we need to validate it as all columns need to be matching
        # for dask. Otherwise, mismatch error is thrown
        origin_files, target_files = [], []
        for f in files:
            if self._validate_file(f, 'origin'):
                origin_files.append(f)
            if self._validate_file(f, 'target'):
                target_files.append(f)
        def batch_files(file_list, batch_size):
            for i in range(0, len(file_list), batch_size):
                yield file_list[i:i+batch_size]
        # Send tasks
        batch_size = max(1, ceil(len(origin_files) / self.num_cores))
        origin_tasks = [
            dask.delayed(lambda fs: pd.concat([MochiStatistics._parse_file(f, 'origin') for f in fs], axis=0))(batch)
            for batch in batch_files(origin_files, batch_size)
        ]   
        target_tasks = [
            dask.delayed(lambda fs: pd.concat([MochiStatistics._parse_file(f, 'target') for f in fs], axis=0))(batch)
            for batch in batch_files(target_files, batch_size)
        ]
        self.origin_rpc_ddf = dd.from_delayed(origin_tasks)
        self.target_rpc_ddf = dd.from_delayed(target_tasks) 

    def _validate_file(self, filename: str, section_name: str):
        # Check if file is empty before trying to parse JSON
        if os.path.getsize(filename) == 0:
            return False
    
        with open(filename, 'rb') as f:
            content = orjson.loads(f.read())
        rpcs = content['rpcs']
        progress_loop = content['progress_loop']
        address = content['address']
        basename = os.path.basename(filename)
        # Check if address field is empty
        if not address or address == '':
            return False
        if section_name == 'origin':
            # Check if origin section exists
            rpcs = {k:v for k, v in rpcs.items() if section_name in v}
            if len(rpcs) == 0:
                return False
        elif section_name == 'target':  
            # Check if target section exists
            rpcs = {k:v for k, v in rpcs.items() if section_name in v}
            if len(rpcs) == 0:
                return False      
            # 'target' section also implies that it received a bulk rpc
            is_target_file = False
            for rpc in rpcs.values():
                section = rpc[section_name]
                for peer_key, operations in section.items():
                    peer_address = peer_key.split()[-1]
                    operations = {k: v for k, v in operations.items() if k != 'bulk'}
                    if len(operations) == 0: 
                        continue
                    is_target_file = True
            if not is_target_file:
                return False
        return True

    """ Static methods are used for serialization when passing it to dask.delayed via lambda """
    @staticmethod
    def _parse_file(filename: str, section_name: str):
        try:
            with open(filename, 'rb') as f:
                content = orjson.loads(f.read())
            rpcs = content['rpcs']
            progress_loop = content['progress_loop']
            address = content['address']
            basename = os.path.basename(filename)
            if section_name == 'origin':
                return MochiStatistics._make_rpc_stats_df(basename, address, rpcs, 'origin', 'sent_to')
            elif section_name == 'target':
                return MochiStatistics._make_rpc_stats_df(basename, address, rpcs, 'target', 'received_from')
            # Unused:
            elif section_name == 'bulk_create':
                return MochiStatistics._make_bulk_create_stats_df(basename, address, rpcs)
            elif section_name == 'bulk_transfer':
                return MochiStatistics._make_bulk_transfer_stats_df(basename, address, rpcs)
            elif section_name == 'progress_loop':
                return MochiStatistics._make_progress_loop_stats_df(basename, address, progress_loop)
            else:
                raise Exception('Invalid option for _parse_file')
        except Exception as e:
            print(f"Error processing file {filename}: {str(e)}")
            return None

    @staticmethod
    def _get_rpc_info(rpc: dict):
        return (rpc['name'], rpc['rpc_id'], rpc['provider_id'],
                rpc['parent_rpc_id'], rpc['parent_provider_id'])

    @staticmethod
    def _make_rpc_stats_df(filename: str, address: str, rpcs: dict, section_name: str, peer_index: str):
        rpcs = {k:v for k, v in rpcs.items() if section_name in v}
        columns = defaultdict(list)
        for rpc in rpcs.values():
            name, rpc_id, provider_id, parent_rpc_id, parent_provider_id = MochiStatistics._get_rpc_info(rpc)
            section = rpc[section_name]
            for peer_key, operations in section.items():
                peer_address = peer_key.split()[-1]
                operations = {k: v for k, v in operations.items() if k != 'bulk'}
                if len(operations) == 0: continue
                # meta columns: use ('meta', '', fieldname) for all meta fields
                columns[('meta', '', 'file')].append(filename)
                columns[('meta', '', 'address')].append(address)
                columns[('meta', '', 'name')].append(name)
                columns[('meta', '', 'rpc_id')].append(rpc_id)
                columns[('meta', '', 'provider_id')].append(provider_id)
                columns[('meta', '', 'parent_rpc_id')].append(parent_rpc_id)
                columns[('meta', '', 'parent_provider_id')].append(parent_provider_id)
                columns[('meta', '', peer_index)].append(peer_address)
                # stats columns: (operation, statsname, statstype)
                for operation, statsblock in operations.items():
                    for statsname, stats in statsblock.items():
                        for statstype, statsval in stats.items():
                            columns[(operation, statsname, statstype)].append(statsval)
        sorted_columns = sorted(columns.keys())
        sorted_column_data = {col: columns[col] for col in sorted_columns}
        pdf = pd.DataFrame(sorted_column_data)
        # Ensure uint64 types to fill all ids
        for col in ['rpc_id', 'parent_rpc_id', 'provider_id', 'parent_provider_id']:
            pdf[('meta', '', col)] = pdf[('meta', '', col)].astype('uint64')
        return pdf
    
    @staticmethod
    def _make_bulk_create_stats_df(filename: str, address: str, rpcs: dict):
        rpcs = {k:v for k, v in rpcs.items() if 'target' in v}
        columns = defaultdict(list)
        for rpc in rpcs.values():
            name, rpc_id, provider_id, parent_rpc_id, parent_provider_id = MochiStatistics._get_rpc_info(rpc)(rpc)
            target = rpc['target']
            for received_from, operations in target.items():
                if 'bulk' not in operations: continue
                if 'create' not in operations['bulk']: continue
                create = operations['bulk']['create']
                received_from_address = received_from.split()[-1]
                # meta columns
                columns[('meta', '', 'file')].append(filename)
                columns[('meta', '', 'address')].append(address)
                columns[('meta', '', 'name')].append(name)
                columns[('meta', '', 'rpc_id')].append(rpc_id)
                columns[('meta', '', 'provider_id')].append(provider_id)
                columns[('meta', '', 'parent_rpc_id')].append(parent_rpc_id)
                columns[('meta', '', 'parent_provider_id')].append(parent_provider_id)
                columns[('meta', '', 'received_from')].append(received_from_address)
                # stats columns
                for statsname, stats in create.items():
                    for statstype, statsval in stats.items():
                        columns[('bulk_create', statsname, statstype)].append(statsval)
        sorted_columns = sorted(columns.keys())
        sorted_column_data = {col: columns[col] for col in sorted_columns}
        pdf = pd.DataFrame(sorted_column_data)
        return pdf

    @staticmethod
    def _make_bulk_transfer_stats_df(filename: str, address: str, rpcs: dict):
        rpcs = {k:v for k, v in rpcs.items() if 'target' in v}
        columns = defaultdict(list)
        for rpc in rpcs.values():
            name, rpc_id, provider_id, parent_rpc_id, parent_provider_id = MochiStatistics._get_rpc_info(rpc)(rpc)
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
                    # meta columns
                    columns[('meta', '', 'file')].append(filename)
                    columns[('meta', '', 'address')].append(address)
                    columns[('meta', '', 'name')].append(name)
                    columns[('meta', '', 'rpc_id')].append(rpc_id)
                    columns[('meta', '', 'provider_id')].append(provider_id)
                    columns[('meta', '', 'parent_rpc_id')].append(parent_rpc_id)
                    columns[('meta', '', 'parent_provider_id')].append(parent_provider_id)
                    columns[('meta', '', 'received_from')].append(received_from_address)
                    columns[('meta', '', 'transfer_type')].append(transfer_type)
                    columns[('meta', '', 'remote_address')].append(peer_address)
                    # stats columns
                    for bulk_operation, statsblock in transfer_stats.items():
                        for statsname, stats in statsblock.items():
                            for statstype, statsval in stats.items():
                                columns[(bulk_operation, statsname, statstype)].append(statsval)
        sorted_columns = sorted(columns.keys())
        sorted_column_data = {col: columns[col] for col in sorted_columns}
        pdf = pd.DataFrame(sorted_column_data)
        return pdf

    @staticmethod
    def _make_progress_loop_stats_df(filename: str, address: str, progress_loop: dict):
        columns = defaultdict(list)
        # meta columns
        for statsblock, stats in progress_loop.items():
            columns[('meta', '', 'file')].append(filename)
            columns[('meta', '', 'address')].append(address)
            # stats columns
            for statskey, statsval in stats.items():
                columns[(statsblock, statskey)].append(statsval)
        sorted_columns = sorted(columns.keys())
        sorted_column_data = {col: columns[col] for col in sorted_columns}
        pdf = pd.DataFrame(sorted_column_data)
        return pdf
