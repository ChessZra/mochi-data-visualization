"""
Preliminary code from 
https://github.com/mochi-hpc/mochi-performance-analysis/
"""

import json
import pandas as pd
import os
from collections import defaultdict
from typing import List


class MochiStatistics:

    def __init__(self, files: List[str] = []):
        self.origin_rpc_df = pd.DataFrame()
        self.target_rpc_df = pd.DataFrame()
        self.bulk_create_df = pd.DataFrame()
        self.bulk_transfer_df = pd.DataFrame()
        self.progress_loop_df = pd.DataFrame()
        for filename in files:
            self.add_file(filename)

    def add_file(self, filename: str):
        content = json.load(open(filename))
        rpcs = content['rpcs']
        progress_loop = content['progress_loop']
        address = content['address']
        basename = os.path.basename(filename)

        self._add_origin_rpc_stats(basename, address, rpcs)
        self._add_target_rpc_stats(basename, address, rpcs)
        self._add_bulk_create_stats(basename, address, rpcs)
        self._add_bulk_transfer_stats(basename, address, rpcs)
        self._add_progress_loop_stats(basename, address, progress_loop)

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
        df = self._make_rpc_stats_df(filename, address, rpcs, 'origin', 'sent_to')
        self.origin_rpc_df = pd.concat([self.origin_rpc_df, df])

    def _add_target_rpc_stats(self, filename: str, address: str, rpcs: dict):
        df = self._make_rpc_stats_df(filename, address, rpcs, 'target', 'received_from')
        self.target_rpc_df = pd.concat([self.target_rpc_df, df])

    def _add_bulk_create_stats(self, filename: str, address: str, rpcs: dict):
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
        columns = defaultdict(list)
        for statsblock, stats in progress_loop.items():
            for statskey, statsval in stats.items():
                columns[(statsblock, statskey)].append(statsval)
        index = pd.MultiIndex.from_tuples([(filename, address)], names=['file', 'address'])
        df = pd.DataFrame(columns, index=index)
        df.columns.name = ('category', 'statistics')
        self.progress_loop_df = pd.concat([self.progress_loop_df, df])
