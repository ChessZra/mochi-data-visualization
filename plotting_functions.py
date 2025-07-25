"""
Mochi Data Visualization - Plotting Functions

This module provides a comprehensive set of functions for visualizing RPC (Remote Procedure Call)
performance data. It follows a consistent methodology for data processing and visualization.

GENERAL METHODOLOGY:
===================

1. DATA ACCESS:
    - All functions take a 'stats' object containing DataFrames (origin_rpc_df, target_rpc_df, bulk_transfer_df)
    - Client-side data: stats.origin_rpc_df (RPC calls made by clients)
    - Server-side data: stats.target_rpc_df (RPC executions on servers)
    - Bulk transfer data: stats.bulk_transfer_df (RDMA data transfers)

2. RPC IDENTIFICATION:
    - RPCs are identified by tuples: (src_address, dst_address, RPC_string)
    - RPC_string format: "source ➔ destination" or just "destination"
    - rpc_id_dict maps RPC names to their numeric IDs
    - rpc_name maps RPC IDs back to their string names

3. DATA FILTERING:
    If we want to filter out the stats dataframe with only the RPC we want:
   - Common pattern: df.xs((parent_rpc_id, rpc_id, src_address, dst_address), level=[...])
        - What this does: Returns a dataframe with that specific RPC where multiple rows are possible due to provider ID's to which aggregation handling is needed.

4. AGGREGATION:
    - Sum durations: df['function']['duration']['sum']
    - Count calls: df['function']['duration']['num']
    - Get averages: df['function']['duration']['avg']
    - Get variance: df['function']['duration']['var']
    - Get min/max: df['function']['duration']['min/max']

5. VISUALIZATION:
    - Use hvplot for pandas DataFrames: df.hvplot.bar(...)
    - Use HoloViews for complex plots: hv.Chord(...)
    - Recommended option: .opts(default_tools=["pan"], shared_axes=False)
    - Standard dimensions: height=500, width=1000

6. ERROR HANDLING:
    - Check if data exists before processing
    - Raise ValueError with descriptive messages when no data is found
    - Use try/except blocks for data access that might fail

7. STATISTICAL AGGREGATION:
    - For combining variances: use the formula for combining variances from different groups
    - For means: weighted average based on number of samples
    - For multiple RPCs: aggregate across all RPCs in rpc_list

8. GRAPH CREATION:
    - Filter data by RPC criteria
    - Aggregate data appropriately (sum, mean, etc.)
    - Create visualization with hvplot or HoloViews
    - Apply standard styling and options
    - Return the plot object

EXTENDING THE CODEBASE:
======================

To add a new graph function:
Example template:


def create_graph_X(stats, rpc_id_dict, rpc_list, **kwargs):
    1. Filter data based on rpc_list
    2. Aggregate data appropriately
    3. Create visualization
    4. Apply styling
    5. Return plot

COMMON DATA STRUCTURES:
======================
    - stats.origin_rpc_df: Multi-index DataFrame with client-side RPC data
    - stats.target_rpc_df: Multi-index DataFrame with server-side RPC data
    - stats.bulk_transfer_df: DataFrame with RDMA transfer data
    - rpc_list: List of (src_address, dst_address, RPC_string) tuples
    - RPC_string: A string of "source ➔ destination" or just "destination" 
    - rpc_id_dict: Dict mapping RPC names to IDs
    - rpc_name: Dict mapping RPC IDs to names

COMMON FUNCTIONS:
================
    - get_src_dst_from_rpc_string(): Parse RPC strings
    - wrap_label(): Format long labels for display
    - get_mean_variance_from_rpcs_client/server(): Statistical aggregation
"""

import holoviews as hv
import hvplot.pandas # For Series
import time
import pandas as pd

from collections import Counter
from holoviews import opts
from mochi_perf.graph import OriginRPCGraph, TargetRPCGraph


class MochiPlotter:

    """ Helper Functions"""
    @staticmethod
    def debug_time(func):
        """
        Simple timing decorator that measures and prints the execution time of decorated functions.
        
        Args:
            func: The function to be timed
            
        Returns:
            wrapper: A wrapped version of the function that prints timing information
        """
        def wrapper(*args, **kwargs):
            start = time.time()
            result = func(*args, **kwargs)
            print(f"{func.__name__}: {time.time() - start:.2f}s")
            return result
        return wrapper

    @staticmethod
    def get_src_dst_from_rpc_string(rpc_string):
        """
        Parse an RPC string to extract source and destination components.
        
        Args:
            rpc_string (str): rpc_string string in format "source ➔ destination" or just "destination"
            
        Returns:
            tuple: (source, destination) where source is 'None' if not specified
        """
        if '➔' in rpc_string:
            src, dest = rpc_string[:rpc_string.index('➔')-1], rpc_string[rpc_string.index('➔')+2:]
        else:
            src, dest = 'None', rpc_string
        return src, dest

    @staticmethod
    def wrap_label(label, width=10):
        """
        Wrap a long label by inserting newlines at specified intervals.
        
        Args:
            label (str): The label text to wrap
            width (int): Maximum characters per line (default: 10)
            
        Returns:
            str: The wrapped label with newlines inserted
        """
        return '\n'.join([label[i:i+width] for i in range(0, len(label), width)])
    
    def get_all_addresses(self, stats):
        """
        Extract all unique addresses from both client and server RPC data.
        
        Args:
            stats: Statistics object containing origin_rpc_df and target_rpc_df
            
        Returns:
            list: Sorted list of all unique addresses found in the data
        """
        address1, address2, address3, address4 = set(), set(), set(), set()
        if stats.origin_rpc_df is not None and not stats.origin_rpc_df.empty:
            address1 = set(stats.origin_rpc_df.index.get_level_values('address'))
            address2 = set(stats.origin_rpc_df.index.get_level_values('sent_to'))

        if stats.target_rpc_df is not None and not stats.target_rpc_df.empty:
            address3 = set(stats.target_rpc_df.index.get_level_values('address'))
            address4 = set(stats.target_rpc_df.index.get_level_values('received_from'))
        return sorted(list(address1 | address2 | address3 | address4))

    @debug_time
    def get_mean_variance_from_rpcs_client(self, stats, rpc_id_dict, rpc_list, functions, aggregations):
        """
        Calculate aggregated mean and variance for client-side RPC functions across multiple RPCs.
        
        This function processes client-side RPC data to compute statistical measures for each
        function step across all specified RPCs. It handles variance aggregation using the
        formula for combining variances from different groups.
        
        Args:
            stats: Statistics object containing origin_rpc_df
            rpc_id_dict (dict): Mapping of RPC names to their IDs
            rpc_list (list): List of tuples (src_address, dst_address, RPC_string)
            functions (list): List of function names to analyze
            aggregations (list): List of aggregation types corresponding to each function
            
        Returns:
            tuple: (mean_client, var_client) where:
                - mean_client (list[float]): Aggregated mean for each function step
                - var_client (list[float]): Aggregated variance for each function step
                
        Raises:
            ValueError: If no data is available for the selected RPCs
        """

        mean_client = [None] * len(functions)
        var_client = [None] * len(functions)

        num_rpc = [[] for _ in range(len(functions))]
        sum_rpc = [[] for _ in range(len(functions))]
        mean_rpc = [[] for _ in range(len(functions))]
        var_rpc = [[] for _ in range(len(functions))]

        for src_address, dst_address, RPC in rpc_list:
            src, dest = self.get_src_dst_from_rpc_string(RPC)

            try:
                df = stats.origin_rpc_df.xs((rpc_id_dict[src], rpc_id_dict[dest], src_address, dst_address), level=[stats.origin_rpc_df.index.names.index('parent_rpc_id'), stats.origin_rpc_df.index.names.index('rpc_id'), stats.origin_rpc_df.index.names.index('address'), stats.origin_rpc_df.index.names.index('sent_to')])
            except:
                continue

            for index, func in enumerate(functions):
                """ For each step of this unique RPC, get the relevant values.
                Note that there are multiple rows for this unique RPC due to the provider ID's
                so we have to aggregate to reduce it to the RPC itself.
                """

                """ Aggregate by provider ID to find the variance of this RPC 
                    n_1 * (v_1 + d_1 ** 2) + n_2 * (v_2 + d_2 ** 2) 
                    / n_1 + n_2 where d_1 = u_1 - u  
                """
                vars = df[func][aggregations[index]]['var'].tolist()
                nums = df[func][aggregations[index]]['num'].tolist()
                means = df[func][aggregations[index]]['avg'].tolist()

                # Use variance formula in terms of variances
                if df[func][aggregations[index]]['num'].sum() == 0:
                    continue
                rpc_mean = df[func][aggregations[index]]['sum'].sum() / df[func][aggregations[index]]['num'].sum()
                d_squared = [(u - rpc_mean) ** 2 for u in means]
                rpc_variance = sum([nums[index] * (vars[index] + d_squared[index]) for index in range(len(vars))]) / sum(nums)

                # For later aggregation by RPC:
                var_rpc[index].append(rpc_variance)
                mean_rpc[index].append(rpc_mean)
                num_rpc[index].append(df[func][aggregations[index]]['num'].sum())
                sum_rpc[index].append(df[func][aggregations[index]]['sum'].sum()) # This is to find the rpc sum later on

        if sum(num_rpc[0]) == 0:
            raise ValueError('No data available: None of the selected RPCs were found in the client-side data for function breakdown. This may mean these RPCs were not issued by the client, were filtered out, or do not exist for your current selection.')

        """ Now, find mean and var by aggregating all the RPC values """
        for index, func in enumerate(functions):
            all_rpc_mean = sum(sum_rpc[index]) / sum(num_rpc[index])
            d_squared = [(u - all_rpc_mean) ** 2 for u in mean_rpc[index]]
            all_rpc_variance = sum([num_rpc[index][j] * (var_rpc[index][j] + d_squared[j]) for j in range(len(num_rpc[index]))]) / sum(num_rpc[index])

            mean_client[index] = all_rpc_mean
            var_client[index] = all_rpc_variance

        return mean_client, var_client

    @debug_time
    def get_mean_variance_from_rpcs_server(self, stats, rpc_id_dict, rpc_list, functions, aggregations):
        """
        Calculate aggregated mean and variance for server-side RPC functions across multiple RPCs.
        
        This function processes server-side RPC data to compute statistical measures for each
        function step across all specified RPCs. It handles variance aggregation using the
        formula for combining variances from different groups.
        
        Args:
            stats: Statistics object containing target_rpc_df
            rpc_id_dict (dict): Mapping of RPC names to their IDs
            rpc_list (list): List of tuples (src_address, dst_address, RPC_string)
            functions (list): List of function names to analyze
            aggregations (list): List of aggregation types corresponding to each function
            
        Returns:
            tuple: (mean_server, var_server) where:
                - mean_server (list[float]): Aggregated mean for each function step
                - var_server (list[float]): Aggregated variance for each function step
                
        Raises:
            ValueError: If no data is available for the selected RPCs
        """

        mean_server = [None] * len(functions)
        var_server = [None] * len(functions)

        num_rpc = [[] for _ in range(len(functions))]
        sum_rpc = [[] for _ in range(len(functions))]
        mean_rpc = [[] for _ in range(len(functions))]
        var_rpc = [[] for _ in range(len(functions))]

        for src_address, dst_address, RPC in rpc_list:
            src, dest = self.get_src_dst_from_rpc_string(RPC)

            try:
                df = stats.target_rpc_df.xs((rpc_id_dict[src], rpc_id_dict[dest], src_address, dst_address), level=[stats.target_rpc_df.index.names.index('parent_rpc_id'), stats.target_rpc_df.index.names.index   ('rpc_id'), stats.target_rpc_df.index.names.index('received_from'), stats.target_rpc_df.index.names.index('address')])
            except:
                continue

            for index, func in enumerate(functions):
                """ For each step of this unique RPC, get the relevant values.
                Note that there are multiple rows for this unique RPC due to the provider ID's
                so we have to aggregate to reduce it to the RPC itself.
                """

                """ Aggregate by provider ID to find the variance of this RPC 
                    n_1 * (v_1 + d_1 ** 2) + n_2 * (v_2 + d_2 ** 2) 
                    / n_1 + n_2 where d_1 = u_1 - u  
                """
                vars = df[func][aggregations[index]]['var'].tolist()
                nums = df[func][aggregations[index]]['num'].tolist()
                means = df[func][aggregations[index]]['avg'].tolist()

                # Use variance formula in terms of variances
                if df[func][aggregations[index]]['num'].sum() == 0:
                    continue
                rpc_mean = df[func][aggregations[index]]['sum'].sum() / df[func][aggregations[index]]['num'].sum()
                d_squared = [(u - rpc_mean) ** 2 for u in means]
                rpc_variance = sum([nums[index] * (vars[index] + d_squared[index]) for index in range(len(vars))]) / sum(nums)

                # For later aggregation by RPC:
                var_rpc[index].append(rpc_variance)
                mean_rpc[index].append(rpc_mean)
                num_rpc[index].append(df[func][aggregations[index]]['num'].sum())
                sum_rpc[index].append(df[func][aggregations[index]]['sum'].sum()) # This is to find the rpc sum later on

        if sum(num_rpc[0]) == 0:
            raise ValueError("No data available: None of the selected RPCs were found in the server-side data. This may mean these RPCs were not executed on the server, were filtered out, or do not exist for your current selection.")

        """ Now, find mean and var by aggregating all the RPC values """
        for index, func in enumerate(functions):
            all_rpc_mean = sum(sum_rpc[index]) / sum(num_rpc[index])
            d_squared = [(u - all_rpc_mean) ** 2 for u in mean_rpc[index]]
            all_rpc_variance = sum([num_rpc[index][j] * (var_rpc[index][j] + d_squared[j]) for j in range(len(num_rpc[index]))]) / sum(num_rpc[index])

            mean_server[index] = all_rpc_mean
            var_server[index] = all_rpc_variance

        return mean_server, var_server

    """ Main Page Plots """
    @debug_time
    def create_graph_1(self, stats):
        """
        Create a bar chart showing total RPC call time by process.
        
        This graph aggregates the total time spent by each client process making RPC calls,
        including iforward duration, wait time, and relative timestamps.
        
        Args:
            stats: Statistics object containing origin_rpc_df
            
        Returns:
            hv.Bars: HoloViews bar chart showing total RPC call time by process
        """
        df = stats.origin_rpc_df

        step_1 = df['iforward']['duration']['sum'].rename('iforward_sum')
        step_2 = df['iforward_wait']['relative_timestamp_from_iforward_end']['sum'].rename('iforward_wait_rel_sum')
        step_3 = df['iforward_wait']['duration']['sum'].rename('iforward_wait_duration_sum')

        merged = pd.concat([step_1, step_2, step_3], axis=1)

        merged['total_sum'] = merged.sum(axis=1)

        aggregate_process_df = merged.groupby('address').agg('sum')

        return aggregate_process_df['total_sum'].sort_values(ascending=False).hvplot.bar(xlabel='Address', ylabel='Total Time', title='Total RPC Call Time by Process (Client Side)', rot=45, height=500, width=1000).opts(default_tools=["pan"], shared_axes=False)

    @debug_time
    def create_graph_2(self, stats):
        """
        Create a bar chart showing total RPC execution time by process.
        
        This graph shows the total time each server process spends executing RPC requests,
        based on the 'ult' duration metric.
        
        Args:
            stats: Statistics object containing target_rpc_df
            
        Returns:
            hv.Bars: HoloViews bar chart showing total RPC execution time by process
        """
        df = stats.target_rpc_df
        aggregate_process_df = df['ult']['duration']['sum'].groupby('address').agg('sum')

        return aggregate_process_df.sort_values(ascending=False).hvplot.bar(xlabel='Address', ylabel='Total Time', title='Total RPC Execution Time by Process (Server Side)', rot=45, height=500, width=1000).opts(default_tools=["pan"], shared_axes=False)

    @debug_time
    def create_graph_3(self, stats, address, rpc_name):
        """
        Create a bar chart showing top 5 RPC call times for a specific process.
        
        This graph shows the top 5 RPC calls made by a specific client process,
        including total time spent on each RPC type.
        
        Args:
            stats: Statistics object containing origin_rpc_df
            address (str): The client process address to analyze
            rpc_name (dict): Mapping of RPC IDs to their names
            
        Returns:
            hv.Bars: HoloViews bar chart showing top 5 RPC call times for the process
        """
        df = stats.origin_rpc_df
        filtered_process = df.xs(address, level='address')

        new_x_labels = []
        for file, name, rpc_id, provider_id, parent_rpc_id, parent_provider_id, sent_to in filtered_process.index:
            new_x_labels.append(self.wrap_label(f'{rpc_name[parent_rpc_id]}\n➔ {rpc_name[rpc_id]}', width=25) if parent_rpc_id != 65535 else self.wrap_label(f'{rpc_name[rpc_id]}', width=25))

        filtered_process.index = new_x_labels
        step_1 = filtered_process['iforward']['duration']['sum'].rename('iforward_sum')
        step_2 = filtered_process['iforward_wait']['relative_timestamp_from_iforward_end']['sum'].rename('iforward_wait_rel_sum')
        step_3 = filtered_process['iforward_wait']['duration']['sum'].rename('iforward_wait_duration_sum')

        merged = pd.concat([step_1, step_2, step_3], axis=1)

        merged['total_sum'] = merged.sum(axis=1)

        return merged['total_sum'].groupby(level=0).agg('sum').sort_values(ascending=False).head(5).hvplot.bar(xlabel='Remote Procedure Calls (RPC)', ylabel='Time', title=f'Top 5 RPC Call Times for {address}', rot=0, height=500, width=1000).opts(default_tools=["pan"], shared_axes=False)

    @debug_time
    def create_graph_4(self, stats, address, rpc_name):
        """
        Create a bar chart showing top 5 RPC execution times for a specific process.
        
        This graph shows the top 5 RPC executions handled by a specific server process,
        including total time spent on each RPC type.
        
        Args:
            stats: Statistics object containing target_rpc_df
            address (str): The server process address to analyze
            rpc_name (dict): Mapping of RPC IDs to their names
            
        Returns:
            hv.Bars: HoloViews bar chart showing top 5 RPC execution times for the process
        """   
        df = stats.target_rpc_df
        filtered_process = df.xs(address, level='address')

        new_x_labels = []
        for file, name, rpc_id, provider_id, parent_rpc_id, parent_provider_id, sent_to in filtered_process.index:
            new_x_labels.append(self.wrap_label(f'{rpc_name[parent_rpc_id]}\n➔ {rpc_name[rpc_id]}', width=25) if parent_rpc_id != 65535 else self.wrap_label(f'{rpc_name[rpc_id]}', width=25))

        filtered_process.index = new_x_labels

        return filtered_process['ult']['duration']['sum'].groupby(level=0).agg('sum').sort_values(ascending=False).head(5).hvplot.bar(xlabel='Remote Procedure Calls (RPC)', ylabel='Time', title=f'Top 5 RPC Execution Times for {address}', rot=0, height=500, width=1000).opts(default_tools=["pan"], shared_axes=False)

    @debug_time
    def create_graph_5(self, stats, metric, rpc_name):
        """
        Create a bar chart showing top 5 busiest RPCs based on selected metric.
        
        This graph shows the top 5 RPCs that consume the most resources based on the
        selected metric (Server Execution Time, Client Call Time, Bulk Transfer Time, or RDMA Data Transfer Size).
        
        Args:
            stats: Statistics object containing various dataframes
            metric (str): The metric to analyze ('Server Execution Time', 'Client Call Time', 
                        'Bulk Transfer Time', or 'RDMA Data Transfer Size')
            rpc_name (dict): Mapping of RPC IDs to their names
            
        Returns:
            hv.Bars: HoloViews bar chart showing top 5 busiest RPCs by the selected metric
            
        Raises:
            Exception: If an invalid metric is provided
        """
        # Configuration mapping for metrics
        metric_config = {
            'Server Execution Time': {
                'df': stats.target_rpc_df["ult"]["duration"]['sum'],
                'title': f'Top 5 Busiest RPCs (total time executing across all servers)'
            },
            'Client Call Time': {
                'df': (stats.origin_rpc_df['iforward']['duration']['sum'] + 
                    stats.origin_rpc_df['iforward_wait']['relative_timestamp_from_iforward_end']['sum'] + 
                    stats.origin_rpc_df['iforward_wait']['duration']['sum']),
                'title': f'Top 5 Busiest RPCs (total time waiting across all clients)',
            },
            'Bulk Transfer Time': {
                'df': (stats.bulk_transfer_df['itransfer']['duration']['sum'] + 
                    stats.bulk_transfer_df['itransfer_wait']['relative_timestamp_from_itransfer_end']['sum'] + 
                    stats.bulk_transfer_df['itransfer_wait']['duration']['sum']),
                'title': f'Total bulk transfer time for this RPC',
            },
            'RDMA Data Transfer Size': {
                'df': stats.bulk_transfer_df['itransfer']['size']['sum'],
                'title': f'Total amount of data transferred using RDMA from this RPC'
            }
        }
        
        if metric not in metric_config:
            raise Exception('Exception, invalid metric passed')
        
        config = metric_config[metric]
        df = config['df']
        title = config['title']
        ylabel = 'Size (in bytes)' if metric == 'RDMA Data Transfer Size' else 'Time (in seconds)'

        # Group by RPC (defined by parent_rpc_id -> rpc_id)
        df = df.groupby(["parent_rpc_id", "rpc_id"]).agg('sum')
        
        # Create new index with RPC names
        df.index = [self.wrap_label(f'{rpc_name[parent_id]}\n➔ {rpc_name[rpc_id]}', width=25) if parent_id != 65535 else self.wrap_label(f'{rpc_name[rpc_id]}', width=25) for parent_id, rpc_id in df.index]
        
        # Create and return the plot
        return df.sort_values(ascending=False).head(5).hvplot.bar(title=title, xlabel='Remote Procedure Calls (RPC)', ylabel=ylabel, rot=0, height=500, width=1000).opts(default_tools=["pan"], shared_axes=False)

    """ Per-RPC Plots """
    @debug_time
    def create_graph_6(self, stats, rpc_id_dict, rpc_list):
        """
        Create a scatter plot with error bars showing top 5 server RPCs by average execution time,
        along with their min and max bounds.

        Args:
            stats: Statistics object containing target_rpc_df
            rpc_id_dict (dict): Mapping of RPC names to their IDs
            rpc_list (list): List of tuples (src_address, dst_address, RPC_string)

        Returns:
            hv.Scatter: HoloViews scatter plot with error bars

        Raises:
            ValueError: If no data is available for the selected RPCs
        """
        data = []
        not_found = []

        for src_address, dst_address, RPC in rpc_list:
            src, dest = self.get_src_dst_from_rpc_string(RPC)

            try:
                df = stats.target_rpc_df.xs(
                    (rpc_id_dict[src], rpc_id_dict[dest], src_address, dst_address),
                    level=[
                        stats.target_rpc_df.index.names.index('parent_rpc_id'),
                        stats.target_rpc_df.index.names.index('rpc_id'),
                        stats.target_rpc_df.index.names.index('received_from'),
                        stats.target_rpc_df.index.names.index('address')
                    ]
                )
            except:
                not_found.append((src_address, dst_address, RPC)) 
                continue

            time_sum = df['handler']['duration']['sum'].sum()
            time_max = df['handler']['duration']['max'].max()
            time_min = df['handler']['duration']['min'].min()
            call_count = df['handler']['duration']['num'].sum()
            time_avg = time_sum / call_count

            label = f'{src} ➔ {dest}\n<{dst_address}>' if src != 'None' else f'{dest}\n<{dst_address}>'
            data.append({
                'RPC': label,
                'avg': time_avg,
                'min': time_min,
                'max': time_max,
                'neg_offset': time_avg - time_min, # for error bar calculation
                'pos_offset': time_max - time_avg,
            })

        if not data:
            raise ValueError("No data available: None of the selected RPCs were found in the server-side data. This may mean these RPCs were not executed on the server, were filtered out, or do not exist for your current selection.")

        df = pd.DataFrame(data)
        df = df.sort_values(by='avg', ascending=False).head(5).reset_index(drop=True)

        scatter = hv.Scatter(df).opts(size=10, color='black', tools=['hover'])
        errors = hv.ErrorBars(df, vdims=['avg', 'neg_offset', 'pos_offset'])

        return (scatter * errors).opts(
            height=500,
            width=1000,
            title='Top 5 Server RPCs: Avg Time with Min–Max Range',
            xlabel='RPC',
            ylabel='Execution Time (seconds)',
            ylim=(0, None),
            default_tools=["pan"],
            shared_axes=False
        )

    @debug_time
    def create_graph_7(self, stats, rpc_id_dict, rpc_list):
        """
        Create a scatter plot with error bars showing top 5 client RPCs by average execution time,
        along with their min and max bounds.

        Args:
            stats: Statistics object containing target_rpc_df
            rpc_id_dict (dict): Mapping of RPC names to their IDs
            rpc_list (list): List of tuples (src_address, dst_address, RPC_string)

        Returns:
            hv.Scatter: HoloViews scatter plot with error bars

        Raises:
            ValueError: If no data is available for the selected RPCs
        """
        data = []
        not_found = []

        for src_address, dst_address, RPC in rpc_list:
            src, dest = self.get_src_dst_from_rpc_string(RPC)
            try:
                df = stats.origin_rpc_df.xs(
                    (rpc_id_dict[src], rpc_id_dict[dest], src_address, dst_address),
                    level=[stats.origin_rpc_df.index.names.index('parent_rpc_id'),
                        stats.origin_rpc_df.index.names.index('rpc_id'),
                        stats.origin_rpc_df.index.names.index('address'),
                        stats.origin_rpc_df.index.names.index('sent_to')]
                )
            except:
                not_found.append((src_address, dst_address, RPC)) 
                continue

            time_sum = df['iforward']['duration']['sum'].sum() + \
                    df['iforward_wait']['relative_timestamp_from_iforward_end']['sum'].sum() + \
                    df['iforward_wait']['duration']['sum'].sum()

            time_max = df['iforward']['duration']['max'].max() + \
                    df['iforward_wait']['relative_timestamp_from_iforward_end']['max'].max() + \
                    df['iforward_wait']['duration']['max'].max()

            time_min = df['iforward']['duration']['min'].min() + \
                    df['iforward_wait']['relative_timestamp_from_iforward_end']['min'].min() + \
                    df['iforward_wait']['duration']['min'].min()

            call_count = df['iforward']['duration']['num'].sum()
            time_avg = time_sum / call_count

            label = f'{src} ➔ {dest}\n<{src_address}>' if src != 'None' else f'{dest}\n<{src_address}>'
            data.append({
                'RPC': label,
                'avg': time_avg,
                'min': time_min,
                'max': time_max,
                'neg_offset': time_avg - time_min, # for error bar calculation
                'pos_offset': time_max - time_avg,
            })

        if not data:
            raise ValueError("No data available: None of the selected RPCs were found in the client-side data. This may mean these RPCs were not issued by the client, were filtered out, or do not exist for your current selection.")

        df = pd.DataFrame(data)
        df = df.sort_values(by='avg', ascending=False).head(5).reset_index(drop=True)

        scatter = hv.Scatter(df).opts(size=10, color='black', tools=['hover'])
        errors = hv.ErrorBars(df, vdims=['avg', 'neg_offset', 'pos_offset'])

        return (scatter * errors).opts(
            height=500,
            width=1000,
            title='Top 5 Client RPCs: Avg Time with Min–Max Range',
            xlabel='RPC',
            ylabel='Call Time (seconds)',
            ylim=(0, None),
            default_tools=["pan"],
            shared_axes=False
        )

    @debug_time
    def create_graph_8(self, stats, rpc_id_dict, rpc_list, view_type):
        """
        Create a bar chart showing total time spent in each RPC step across all selected RPCs.
        
        This graph aggregates the total time spent in each step of the RPC process
        across all selected RPCs, helping identify bottlenecks.
        
        Args:
            stats: Statistics object containing origin_rpc_df and target_rpc_df
            rpc_id_dict (dict): Mapping of RPC names to their IDs
            rpc_list (list): List of tuples (src_address, dst_address, RPC_string)
            view_type (string): which functions to graph depending on view_type

        Returns:
            hv.Bars: HoloViews bar chart showing total duration by RPC step
            
        Raises:
            ValueError: If no data is available for the selected RPCs
        """
        if view_type == 'servers':
            functions_server = ['handler', 'ult', 'irespond', 'respond_cb', 'irespond_wait', 'set_output', 'get_input']
            values_server = [0] * len(functions_server)
        else:
            functions_client = ['iforward', 'forward_cb', 'iforward_wait', 'set_input', 'get_output']
            values_client = [0] * len(functions_client)
        
        for src_address, dst_address, RPC in rpc_list:
            src, dest = self.get_src_dst_from_rpc_string(RPC)

            if view_type == 'servers':
                try:
                    # Get RPC server-side
                    df = stats.target_rpc_df.xs((rpc_id_dict[src], rpc_id_dict[dest], src_address, dst_address), level=[stats.target_rpc_df.index.names.index('parent_rpc_id'), stats.target_rpc_df.index.names.index('rpc_id'), stats.target_rpc_df.index.names.index('received_from'), stats.target_rpc_df.index.names.index('address')])
                    for index, func in enumerate(functions_server): # We are summing because there are still provider ID's
                        values_server[index] += df[func]['duration']['sum'].sum()
                except:
                    pass
            else:
                try:
                    # Get RPC client-side
                    df = stats.origin_rpc_df.xs((rpc_id_dict[src], rpc_id_dict[dest], src_address, dst_address), level=[stats.origin_rpc_df.index.names.index('parent_rpc_id'), stats.origin_rpc_df.index.names.index('rpc_id'), stats.origin_rpc_df.index.names.index('address'), stats.origin_rpc_df.index.names.index('sent_to')])
                    for index, func in enumerate(functions_client):
                        values_client[index] += df[func]['duration']['sum'].sum()
                except:
                    pass
                
        title = "Total Time Spent in Each RPC Step (Aggregated Across All Selected RPCs)"
        x_label = "RPC Step"
        y_label = "Total Duration (s, aggregated)"

        if view_type == 'servers':
            if sum(values_server) == 0:
                raise ValueError("No data available: None of the selected RPCs were found in the server-side data. This may mean these RPCs were not executed on the server, were filtered out, or do not exist for your current selection.")
            
            target_plot_df = pd.DataFrame({'Function': functions_server, 'Total Duration': values_server, 'color':'#ff7f0e'}).sort_values(by='Total Duration', ascending=False)
            return target_plot_df.hvplot.bar(x='Function', y='Total Duration', title=title, rot=45, color='color', xlabel=x_label, ylabel=y_label, height=500, width=1000).opts(shared_axes=False, default_tools=["pan"]) 
        else:
            if sum(values_client) == 0:
                raise ValueError("No data available: None of the selected RPCs were found in the client-side data. This may mean these RPCs were not issued by the client, were filtered out, or do not exist for your current selection.")
            origin_plot_df = pd.DataFrame({'Function': functions_client, 'Total Duration': values_client, 'color': '#1f77b4'}).sort_values(by='Total Duration', ascending=False)
            return origin_plot_df.hvplot.bar(x='Function', y='Total Duration', title=title, rot=45, color='color', xlabel=x_label, ylabel=y_label, height=500, width=1000).opts(shared_axes=False, default_tools=["pan"]) 

    @debug_time
    def create_graph_9(self, stats, rpc_id_dict, rpc_list):
        """
        Create a bar chart with error bars showing server-side RPC function statistics.
        
        This graph shows the mean duration and standard deviation for each server-side
        RPC function step, with error bars indicating the variability in execution times.
        
        Args:
            stats: Statistics object containing target_rpc_df
            rpc_id_dict (dict): Mapping of RPC names to their IDs
            rpc_list (list): List of tuples (src_address, dst_address, RPC_string)
            
        Returns:
            hv.Overlay: HoloViews overlay of bar chart and error bars
            
        Raises:
            ValueError: If no data is available for the selected RPCs
        """
        
        functions_server = ['handler', 'ult', 'irespond', 'respond_cb', 'irespond_wait', 'set_output', 'get_input']
        aggregations = ['duration', 'duration', 'duration', 'duration', 'duration', 'duration', 'duration']
        try:
            mean_server, var_server = self.get_mean_variance_from_rpcs_server(stats, rpc_id_dict, rpc_list, functions_server, aggregations)
        except ValueError as e:
            raise ValueError(str(e))  # throw it again, loud and proud

        std_server = [v ** 0.5 for v in var_server]

        # Color for bars (can also be a colormap or hex list)
        colors = ['#ff7f0e'] * len(functions_server)

        # Create the base DataFrame
        target_plot_df = pd.DataFrame({
            'Function': functions_server,
            'Mean Duration': mean_server,
            'Std Dev': std_server,
            'color': colors
        }).sort_values(by='Mean Duration', ascending=False)

        # Build the bar chart
        bar_plot = target_plot_df.hvplot.bar(
            x='Function',
            y='Mean Duration',
            title="Aggregated RPC Duration per Function (Mean ± Std Dev)",
            rot=45,
            color='color',
            xlabel='Function',
            ylabel='Mean Duration (s)',
            height=500,
            width=1000,
            hover_cols=['Std Dev']
        )
        
        # Build the error bars using HoloViews
        error_data = [(func, mean, std) for func, mean, std in zip(functions_server, mean_server, std_server)]
        error_plot = hv.ErrorBars(error_data, kdims=['Function'], vdims=['Mean Duration', 'Std Dev'])

        return (bar_plot * error_plot).opts(shared_axes=False, default_tools=["pan"], ylim=(0, None))

    @debug_time
    def create_graph_10(self, stats, rpc_id_dict, rpc_list):
        """
        Create a bar chart with error bars showing client-side RPC function statistics.
        
        This graph shows the mean duration and standard deviation for each client-side
        RPC function step, with error bars indicating the variability in call times.
        
        Args:
            stats: Statistics object containing origin_rpc_df
            rpc_id_dict (dict): Mapping of RPC names to their IDs
            rpc_list (list): List of tuples (src_address, dst_address, RPC_string)
            
        Returns:
            hv.Overlay: HoloViews overlay of bar chart and error bars
            
        Raises:
            ValueError: If no data is available for the selected RPCs
        """

        functions_client = ['iforward', 'forward_cb', 'iforward_wait', 'set_input', 'get_output']
        aggregations = ['duration', 'duration', 'duration', 'duration', 'duration']
        try:
            mean_client, var_client = self.get_mean_variance_from_rpcs_client(stats, rpc_id_dict, rpc_list, functions_client, aggregations)
        except ValueError as e:
            raise ValueError(str(e))  # throw it again, loud and proud
        
        std_server = [v ** 0.5 for v in var_client]

        # Color for bars 
        colors = ['#1f77b4'] * len(functions_client)

        # Create the base DataFrame
        origin_plot_df = pd.DataFrame({
            'Function': functions_client,
            'Mean Duration': mean_client,
            'Std Dev': std_server,
            'color': colors
        }).sort_values(by='Mean Duration', ascending=False)

        # Build the bar chart
        bar_plot = origin_plot_df.hvplot.bar(
            x='Function',
            y='Mean Duration',
            title="Aggregated RPC Duration per Function (Mean ± Std Dev)",
            rot=45,
            color='color',
            xlabel='Function',
            ylabel='Mean Duration (s)',
            height=500,
            width=1000,
            hover_cols=['Std Dev']
        )

        # Build the error bars using HoloViews
        error_data = [(func, mean, std) for func, mean, std in zip(functions_client, mean_client, std_server)]
        error_plot = hv.ErrorBars(error_data, kdims=['Function'], vdims=['Mean Duration', 'Std Dev'])

        return (bar_plot * error_plot).opts(shared_axes=False, default_tools=["pan"], ylim=(0, None))

    @debug_time
    def create_chord_graph(self, stats, rpc_id_dict, rpc_list, view_type='clients'):
        """
        Create a chord diagram showing RPC communication patterns between processes.
        
        This graph visualizes the flow of RPC calls between different processes,
        showing which processes communicate with each other and the volume of communication.
        
        Args:
            stats: Statistics object containing origin_rpc_df and target_rpc_df
            rpc_id_dict (dict): Mapping of RPC names to their IDs
            rpc_list (list): List of tuples (src_address, dst_address, RPC_string)
            view_type (str): Either 'clients' or 'servers' to determine data source
            
        Returns:
            hv.Chord: HoloViews chord diagram showing communication patterns
            
        Raises:
            ValueError: If no data is available for the selected RPCs
        """

        f = Counter() # (src, dest): weight -> where src and dest are processes, and weight is the total duration (depending on view_type)
        unique_nodes = set()

        for src_address, dst_address, RPC in rpc_list:
            src, dest = self.get_src_dst_from_rpc_string(RPC)
            
            try:
                if view_type == 'clients':
                    df = stats.origin_rpc_df.xs((rpc_id_dict[src], rpc_id_dict[dest], src_address, dst_address), level=[stats.origin_rpc_df.index.names.index('parent_rpc_id'), stats.origin_rpc_df.index.names.index('rpc_id'), stats.origin_rpc_df.index.names.index('address'), stats.origin_rpc_df.index.names.index('sent_to')], drop_level=False)
                else:
                    df = stats.target_rpc_df.xs((rpc_id_dict[src], rpc_id_dict[dest], src_address, dst_address), level=[stats.target_rpc_df.index.names.index('parent_rpc_id'), stats.target_rpc_df.index.names.index('rpc_id'), stats.target_rpc_df.index.names.index('received_from'), stats.target_rpc_df.index.names.index('address')], drop_level=False)
            except:
                continue

            for index, row in df.iterrows():
                if view_type == 'servers':
                    address, received_from = index[1], index[7]
                    f[(received_from, address)] += row['ult']['duration']['sum']
                    unique_nodes.add(received_from)
                else:
                    address, sent_to = index[1], index[7]
                    f[(address, sent_to)] += (row['iforward']['duration']['sum'] + row['iforward_wait']['relative_timestamp_from_iforward_end']['sum'] + row['iforward_wait']['duration']['sum'])
                    unique_nodes.add(sent_to)
                unique_nodes.add(address)

        if not unique_nodes:
            if view_type == 'clients':
                raise ValueError("No data available: None of the selected RPCs were found in the client-side data. This may mean these RPCs were not issued by the client, were filtered out, or do not exist for your current selection.")
            else:
                raise ValueError("No data available: None of the selected RPCs were found in the server-side data. This may mean these RPCs were not executed on the server, were filtered out, or do not exist for your current selection.")
                
        address_to_node_id = {}
        for index, node in enumerate(list(unique_nodes)):
            address_to_node_id[node] = index

        edges = []
        for key, value in f.items():
            u, v = key
            w = value
            edges.append({'source': address_to_node_id[u], 'target': address_to_node_id[v], 'weight': w})

        # Create the network dataframe for Chord
        chord_df = pd.DataFrame(edges)

        # Create the data dataframe for Chord opts
        nodes_df = pd.DataFrame([
            {
                'index': address_to_node_id[node],
                'index_name': node,
                'index_color': i        
            }
            for i, node in enumerate(unique_nodes)
        ])

        dataset = hv.Dataset(nodes_df, 'index')

        return hv.Chord((chord_df, dataset)).opts(
            opts.Chord(
                cmap='Category20',
                edge_cmap='Category20',
                node_color='index_color',
                edge_color='source',
                labels='index_name',
                width=700,
                height=700
            )
        ).opts(shared_axes=False)

    @debug_time
    def create_rpc_load_heatmap(self, stats, rpc_id_dict, rpc_list, view_type='clients'):
        """
        Create a heatmap showing RPC load distribution by clients or servers.
        
        This graph visualizes the distribution of RPC calls across different processes,
        showing which processes are handling which types of RPCs and the volume of each.
        
        Args:
            stats: Statistics object containing origin_rpc_df and target_rpc_df
            rpc_id_dict (dict): Mapping of RPC names to their IDs
            rpc_list (list): List of tuples (src_address, dst_address, RPC_string)
            view_type (str): Either 'clients' or 'servers' to determine data source
            
        Returns:
            hv.HeatMap: HoloViews heatmap showing RPC load distribution
            
        Raises:
            ValueError: If no data is available for the selected RPCs
        """
        rpcs = []
        for src_address, dst_address, RPC in rpc_list:
            src, dest = self.get_src_dst_from_rpc_string(RPC)
            try:
                if view_type == 'clients':
                    df = stats.origin_rpc_df.xs((rpc_id_dict[src], rpc_id_dict[dest], src_address, dst_address), level=[stats.origin_rpc_df.index.names.index('parent_rpc_id'), stats.origin_rpc_df.index.names.index('rpc_id'), stats.origin_rpc_df.index.names.index('address'), stats.origin_rpc_df.index.names.index('sent_to')])
                    rpcs.append({'name': dest,'address': src_address, 'num': df['iforward']['duration']['num'].sum()})
                else:
                    df = stats.target_rpc_df.xs((rpc_id_dict[src], rpc_id_dict[dest], src_address, dst_address), level=[stats.target_rpc_df.index.names.index('parent_rpc_id'), stats.target_rpc_df.index.names.index('rpc_id'), stats.target_rpc_df.index.names.index('received_from'), stats.target_rpc_df.index.names.index('address')])
                    rpcs.append({'name': dest,'address': dst_address, 'num': df['handler']['duration']['num'].sum()})
            except:
                continue
        if not rpcs: # Not found in any files
            raise ValueError(f"No data available: None of the selected RPCs were found in the {'client-side' if view_type == 'clients' else 'server-side'} data for the heatmap. This may mean these RPCs were not recorded, were filtered out, or do not exist for your current selection.")
        
        df = pd.DataFrame(rpcs).set_index(['name', 'address']).groupby(['name', 'address']).sum()['num']        
        df_unstacked = df.unstack(fill_value=0)

        # Sort rows (RPC types) by total activity descending
        row_sums = df_unstacked.sum(axis=1)
        df_unstacked = df_unstacked.loc[row_sums.sort_values(ascending=True).index]

        # Sort columns (addresses) by total activity descending
        col_sums = df_unstacked.sum(axis=0)
        df_unstacked = df_unstacked.loc[:, col_sums.sort_values(ascending=False).index]

        # Now plot  
        heatmap = df_unstacked.hvplot.heatmap(
            title=f'RPC Load Distribution by {view_type.capitalize()}',
            xlabel=f'{self.wrap_label(view_type.capitalize())}',
            ylabel='RPC Type',  
            cmap='viridis',
            rot=45,
            width=1000,
            height=500
        )    
        heatmap.opts(default_tools=['hover'])
        heatmap.opts(shared_axes=False, default_tools=["pan"]) 
        return heatmap

    @debug_time
    def create_per_rpc_svg_origin(self, stats, rpc_id_dict, rpc_list):
        """
        Create an SVG visualization of client-side RPC execution timeline.
        
        This function generates an SVG diagram showing the relative timing and duration
        of each step in the client-side RPC process, normalized to show the complete flow.
        
        Args:
            stats: Statistics object containing origin_rpc_df
            rpc_id_dict (dict): Mapping of RPC names to their IDs
            rpc_list (list): List of tuples (src_address, dst_address, RPC_string)
            
        Returns:
            str: SVG string representation of the client-side RPC timeline
        """
        # Get mean and variance
        functions = ['iforward', 'forward_cb', 'iforward_wait', 'set_input', 'get_output']
        aggregations = ['duration', 'duration', 'duration', 'duration', 'duration']
        means, vars = self.get_mean_variance_from_rpcs_client(stats, rpc_id_dict, rpc_list, functions, aggregations)

        iforward_duration = means[0]
        forward_cb_duration = means[1]
        wait_duration = means[2]
        set_input_duration = means[3]
        get_output_duration = means[4]

        functions = ['iforward', 'set_input', 'iforward_wait', 'forward_cb', 'get_output']
        aggregations = ['relative_timestamp_from_create', 'relative_timestamp_from_iforward_start', 'relative_timestamp_from_iforward_end', 'relative_timestamp_from_iforward_start', 'relative_timestamp_from_wait_end']
        means, vars = self.get_mean_variance_from_rpcs_client(stats, rpc_id_dict, rpc_list, functions, aggregations)

        iforward_relative_create = means[0]
        set_input_relative_iforward_start = means[1]
        wait_relative_iforward_end = means[2]
        forward_cb_relative_iforward_start = means[3]
        get_output_relative_wait_end = means[4]

        iforward_start = iforward_relative_create
        set_input_start = iforward_start + set_input_relative_iforward_start
        wait_start = iforward_start + iforward_duration + wait_relative_iforward_end
        forward_cb_start = iforward_start + forward_cb_relative_iforward_start
        get_output_start = wait_start + wait_duration + get_output_relative_wait_end

        total_duration = max(
            iforward_start + iforward_duration,
            set_input_start + set_input_duration,
            wait_start + wait_duration,
            forward_cb_start + forward_cb_duration,
            get_output_start + get_output_duration,
        )

        # Normalize all values from 0 to 1
        origin_rpc_graph = OriginRPCGraph(
            iforward={'start': iforward_start / total_duration, 'duration': iforward_duration / total_duration},
            set_input={'start': set_input_start / total_duration, 'duration': set_input_duration / total_duration},
            wait={'start': wait_start / total_duration, 'duration': wait_duration / total_duration},
            forward_cb={'start': forward_cb_start / total_duration, 'duration': forward_cb_duration / total_duration},
            get_output={'start': get_output_start / total_duration, 'duration': get_output_duration / total_duration})

        return origin_rpc_graph.to_ipython_svg()   

    @debug_time 
    def create_per_rpc_svg_target(self, stats, rpc_id_dict, rpc_list):
        """
        Create an SVG visualization of server-side RPC execution timeline.
        
        This function generates an SVG diagram showing the relative timing and duration
        of each step in the server-side RPC process, normalized to show the complete flow.
        
        Args:
            stats: Statistics object containing target_rpc_df
            rpc_id_dict (dict): Mapping of RPC names to their IDs
            rpc_list (list): List of tuples (src_address, dst_address, RPC_string)
            
        Returns:
            str: SVG string representation of the server-side RPC timeline
        """
        # Get mean and variance
        functions = ['handler', 'ult', 'irespond', 'respond_cb', 'irespond_wait', 'set_output', 'get_input']
        aggregations = ['duration', 'duration', 'duration', 'duration', 'duration', 'duration', 'duration']
        means, vars = self.get_mean_variance_from_rpcs_server(stats, rpc_id_dict, rpc_list, functions, aggregations)

        handler_duration = means[0]
        ult_duration = means[1]
        irespond_duration = means[2]
        respond_cb_duration = means[3]
        wait_duration = means[4]
        set_output_duration = means[5]
        get_input_duration = means[6]

        # Get mean and variance
        functions = ['ult', 'get_input', 'irespond', 'set_output', 'irespond_wait', 'respond_cb']
        aggregations = ['relative_timestamp_from_handler_start', 'relative_timestamp_from_ult_start', 'relative_timestamp_from_ult_start', 'relative_timestamp_from_irespond_start', 'relative_timestamp_from_irespond_end', 'relative_timestamp_from_irespond_start']
        means, vars = self.get_mean_variance_from_rpcs_server(stats, rpc_id_dict, rpc_list, functions, aggregations)

        ult_relative_handler_start = means[0]
        get_input_relative_ult_start = means[1]
        irespond_relative_ult_start = means[2]
        set_output_relative_irespond_start = means[3]
        wait_relative_irespond_end = means[4]
        respond_cb_relative_irespond_start = means[5]

        ult_start = ult_relative_handler_start
        get_input_start = ult_start + get_input_relative_ult_start
        irespond_start = ult_start + irespond_relative_ult_start
        set_output_start = irespond_start + set_output_relative_irespond_start
        wait_start = irespond_start + irespond_duration + wait_relative_irespond_end
        respond_cb_start = irespond_start + respond_cb_relative_irespond_start

        total_duration = max(
            handler_duration,
            ult_start + ult_duration,
            get_input_start + get_input_duration,
            irespond_start + irespond_duration,
            set_output_start + set_output_duration,
            wait_start + wait_duration,
            respond_cb_start + respond_cb_duration
        )

        # Normalize all values from 0 to 1
        target_rpc_graph = TargetRPCGraph(
            handler={'start': 0.0, 'duration': handler_duration / total_duration},
            ult={'start': ult_start / total_duration, 'duration': ult_duration / total_duration},
            get_input={'start': get_input_start / total_duration, 'duration': get_input_duration / total_duration},
            irespond={'start': irespond_start / total_duration, 'duration': irespond_duration / total_duration},
            set_output={'start': set_output_start / total_duration, 'duration': set_output_duration / total_duration},
            wait={'start': wait_start / total_duration, 'duration': wait_duration / total_duration},
            respond_cb={'start': respond_cb_start / total_duration, 'duration': respond_cb_duration / total_duration}
        )

        return target_rpc_graph.to_ipython_svg()

    """ Plot Descriptions """
    def get_heatmap_description(self, view_type):
        """
        Get a description for the RPC load heatmap based on the view type.
        
        Args:
            view_type (str): Either 'clients' or 'servers' to determine description
            
        Returns:
            str: Description text explaining what the heatmap shows and how to interpret it
        """
        if view_type == 'clients':
            return (
                "**What this shows:** This heatmap lets you quickly spot which client processes are making the most RPC calls and which types of RPCs they use most often. "
                "Use this to find your busiest clients and see if the load is balanced."
            )
        else:
            return (
                "**What this shows:** This heatmap shows which server processes are handling the most RPC requests and which RPC types are most common. "
                "Look for hotspots to find overloaded servers or popular RPCs."
            )

    def get_graph_1_description(self):
        """
        Get a description for graph 1 (Total RPC Call Time by Process).
        
        Returns:
            str: Description text explaining what the graph shows and how to interpret it
        """
        return (
            "**What this shows:** See which processes are the most active clients in your system. "
            "Higher bars mean a process is making more RPC calls to others. Use this to spot your busiest clients."
        )

    def get_graph_2_description(self):
        """
        Get a description for graph 2 (Total RPC Execution Time by Process).
        
        Returns:
            str: Description text explaining what the graph shows and how to interpret it
        """
        return (
            "**What this shows:** Find out which processes are doing the most work as servers. "
            "Higher bars mean a process is handling more RPC requests. This helps you spot overloaded servers."
        )

    def get_graph_3_description(self):
        """
        Get a description for graph 3 (Top 5 RPC Call Times for a specific process).
        
        Returns:
            str: Description text explaining what the graph shows and how to interpret it
        """
        return (
            "**What this shows:** For the selected process, see how much time it spends calling each type of RPC. "
            "This helps you understand what kinds of work your client is doing most."
        )

    def get_graph_4_description(self):
        """
        Get a description for graph 4 (Top 5 RPC Execution Times for a specific process).
        
        Returns:
            str: Description text explaining what the graph shows and how to interpret it
        """
        return (
            "**What this shows:** For the selected process, see how much time it spends handling each type of incoming RPC. "
            "This reveals what kinds of requests your server is working on most."
        )

    def get_graph_5_description(self):
        """
        Get a description for graph 5 (Top 5 Busiest RPCs by metric).
        
        Returns:
            str: Description text explaining what the graph shows and how to interpret it
        """
        return (
            "**What this shows:** These are the top 5 RPCs using the most resources, based on your selected metric. "
            "Use this to quickly find which RPCs are slowing things down or using the most bandwidth."
        )

    def get_graph_6_description(self):
        """
        Get a description for graph 6 (Top 5 Server RPCs by Average Execution Time).
        
        Returns:
            str: Description text explaining what the graph shows and how to interpret it
        """
        return (
            "**What this shows:** These are the top 5 server-side RPCs with the highest average execution time. "
            "For each, you can see the max, average, and min times. Use this to find slow server operations to optimize."
        )

    def get_graph_7_description(self):
        """
        Get a description for graph 7 (Top 5 Client RPCs by Average Call Time).
        
        Returns:
            str: Description text explaining what the graph shows and how to interpret it
        """
        return (
            "**What this shows:** These are the top 5 client-side RPCs with the highest average call time. "
            "For each, you can see the max, average, and min times. Use this to spot slow client operations or network delays."
        )

    def get_graph_8_description(self):
        """
        Get a description for graph 8 (Total Time Spent in Each RPC Step).
        
        Returns:
            str: Description text explaining what the graph shows and how to interpret it
        """
        return (
            "**What this shows:** See how much total time is spent in each step of the RPC process, across all selected RPCs. "
            "Taller bars mean more time spent in that step. Focus on the tallest bars to find bottlenecks."
        )