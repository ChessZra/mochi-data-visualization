"""
Mochi Data Visualization - Plotting Functions

This module provides a comprehensive set of functions for visualizing RPC (Remote Procedure Call)
performance data. It follows a consistent methodology for data processing and visualization.
"""

import dask.dataframe as dd
import holoviews as hv
import hvplot.pandas # For Series
import pandas as pd
import time

from collections import Counter
from functools import lru_cache
from holoviews import opts
from mochi_perf.graph import OriginRPCGraph, TargetRPCGraph


class MochiPlotter:

    def __init__(self, stats):
        self.stats = stats
        # The ordering of this index is expected:
        columns_to_move = [('meta', '', 'file'), ('meta', '', 'address'), ('meta', '', 'name'), ('meta', '', 'rpc_id'), ('meta', '', 'provider_id'), ('meta', '', 'parent_rpc_id'), ('meta', '', 'parent_provider_id'), ('meta', '', 'sent_to')]
        self.origin_rpc_df = self._convert_columns_to_multiindex(stats.origin_rpc_ddf.compute(), columns_to_move)
        columns_to_move = [('meta', '', 'file'), ('meta', '', 'address'), ('meta', '', 'name'), ('meta', '', 'rpc_id'), ('meta', '', 'provider_id'), ('meta', '', 'parent_rpc_id'), ('meta', '', 'parent_provider_id'), ('meta', '', 'received_from')]
        self.target_rpc_df = self._convert_columns_to_multiindex(stats.target_rpc_ddf.compute(), columns_to_move)
        self.rpc_name_dict = {65535: 'None'}
        self.rpc_id_dict = {'None': 65535}
        for df in [self.origin_rpc_df, self.target_rpc_df]:
            for index in df.index:
                self.rpc_name_dict[index[3]], self.rpc_id_dict[index[2]] = index[2], index[3]

    def _convert_columns_to_multiindex(self, df, columns_to_move):
        # Create temporary column names to use with set_index
        temp_names = [f"temp_{i}" for i in range(len(columns_to_move))]
        # Extract the columns and create a temporary DataFrame
        temp_df = df.copy()
        for i, col in enumerate(columns_to_move):
            temp_df[temp_names[i]] = df[col]
        # Set these as index
        temp_df = temp_df.set_index(temp_names)
        # Rename the index levels
        final_names = [col[-1] for col in columns_to_move]  # Use the last part of the tuple as name
        temp_df.index.names = final_names
        # Remove original columns
        temp_df = temp_df.drop(columns=columns_to_move)
        return temp_df

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
    
    def get_all_addresses(self):
        """
        Extract all unique addresses from both client and server RPC data.
        
        Args:
            stats: Statistics object containing origin_rpc_df and target_rpc_df
            
        Returns:
            list: Sorted list of all unique addresses found in the data
        """
        address1, address2, address3, address4 = set(), set(), set(), set()
        if self.origin_rpc_df is not None and not self.origin_rpc_df.empty:
            address1 = set(self.origin_rpc_df.index.get_level_values('address'))
            address2 = set(self.origin_rpc_df.index.get_level_values('sent_to'))
        if self.target_rpc_df is not None and not self.target_rpc_df.empty:
            address3 = set(self.target_rpc_df.index.get_level_values('address'))
            address4 = set(self.target_rpc_df.index.get_level_values('received_from'))
        return sorted(list(address1 | address2 | address3 | address4))

    """ Main Page Plots """
    @debug_time
    def create_graph_1(self):
        """
        Create a bar chart showing total RPC call time by process.
        
        This graph aggregates the total time spent by each client process making RPC calls,
        including iforward duration, wait time, and relative timestamps.
        """
        df = self.origin_rpc_df
        step_1 = df['iforward']['duration']['sum'].rename('iforward_sum')
        step_2 = df['iforward_wait']['relative_timestamp_from_iforward_end']['sum'].rename('iforward_wait_rel_sum')
        step_3 = df['iforward_wait']['duration']['sum'].rename('iforward_wait_duration_sum')
        merged = pd.concat([step_1, step_2, step_3], axis=1)
        merged['total_sum'] = merged.sum(axis=1)
        aggregate_process_df = merged.groupby('address').agg('sum')
        return aggregate_process_df['total_sum'].sort_values(ascending=False).hvplot.bar(xlabel='Address', ylabel='Total Time', title='Total RPC Call Time by Process (Client Side)', rot=45, height=500, width=1000).opts(shared_axes=False)

    @debug_time
    def create_graph_2(self):
        """
        Create a bar chart showing total RPC execution time by process.
        
        This graph shows the total time each server process spends executing RPC requests,
        based on the 'ult' duration metric.
        """
        df = self.target_rpc_df
        aggregate_process_df = df['ult']['duration']['sum'].groupby('address').agg('sum')
        return aggregate_process_df.sort_values(ascending=False).hvplot.bar(xlabel='Address', ylabel='Total Time', title='Total RPC Execution Time by Process (Server Side)', rot=45, height=500, width=1000).opts(shared_axes=False)

    @debug_time
    def create_graph_3(self, address):
        """
        Create a bar chart showing top 5 RPC call times for a specific process.
        
        This graph shows the top 5 RPC calls made by a specific client process,
        including total time spent on each RPC type.
        """
        df = self.origin_rpc_df
        filtered_process = df.xs(address, level='address')
        new_x_labels = []
        for file, name, rpc_id, provider_id, parent_rpc_id, parent_provider_id, sent_to in filtered_process.index:
            new_x_labels.append(self.wrap_label(f'{self.rpc_name_dict[parent_rpc_id]}\n➔ {self.rpc_name_dict[rpc_id]}', width=25) if parent_rpc_id != 65535 else self.wrap_label(f'{self.rpc_name_dict[rpc_id]}', width=25))
        filtered_process.index = new_x_labels
        step_1 = filtered_process['iforward']['duration']['sum'].rename('iforward_sum')
        step_2 = filtered_process['iforward_wait']['relative_timestamp_from_iforward_end']['sum'].rename('iforward_wait_rel_sum')
        step_3 = filtered_process['iforward_wait']['duration']['sum'].rename('iforward_wait_duration_sum')
        merged = pd.concat([step_1, step_2, step_3], axis=1)
        merged['total_sum'] = merged.sum(axis=1)
        return merged['total_sum'].groupby(level=0).agg('sum').sort_values(ascending=False).head(5).hvplot.bar(xlabel='Remote Procedure Calls (RPC)', ylabel='Time', title=f'Top 5 RPC Call Times for {address}', rot=0, height=500, width=1000).opts(shared_axes=False)

    @debug_time
    def create_graph_4(self, address):
        """
        Create a bar chart showing top 5 RPC execution times for a specific process.
        
        This graph shows the top 5 RPC executions handled by a specific server process,
        including total time spent on each RPC type.
        """   
        df = self.target_rpc_df
        filtered_process = df.xs(address, level='address')
        new_x_labels = []
        for file, name, rpc_id, provider_id, parent_rpc_id, parent_provider_id, sent_to in filtered_process.index:
            new_x_labels.append(self.wrap_label(f'{self.rpc_name_dict[parent_rpc_id]}\n➔ {self.rpc_name_dict[rpc_id]}', width=25) if parent_rpc_id != 65535 else self.wrap_label(f'{self.rpc_name_dict[rpc_id]}', width=25))
        filtered_process.index = new_x_labels
        return filtered_process['ult']['duration']['sum'].groupby(level=0).agg('sum').sort_values(ascending=False).head(5).hvplot.bar(xlabel='Remote Procedure Calls (RPC)', ylabel='Time', title=f'Top 5 RPC Execution Times for {address}', rot=0, height=500, width=1000).opts(shared_axes=False)

    @debug_time
    def create_graph_5(self, metric):
        """
        Create a bar chart showing top 5 busiest RPCs based on selected metric.
        
        This graph shows the top 5 RPCs that consume the most resources based on the
        selected metric (Server Execution Time, Client Call Time)
        """
        # Configuration mapping for metrics
        metric_config = {
            'Server Execution Time': {
                'df': self.target_rpc_df["ult"]["duration"]['sum'],
                'title': f'Top 5 Busiest RPCs (total time executing across all servers)'
            },
            'Client Call Time': {
                'df': (self.origin_rpc_df['iforward']['duration']['sum'] + 
                    self.origin_rpc_df['iforward_wait']['relative_timestamp_from_iforward_end']['sum'] + 
                    self.origin_rpc_df['iforward_wait']['duration']['sum']),
                'title': f'Top 5 Busiest RPCs (total time waiting across all clients)',
            },
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
        df.index = [self.wrap_label(f'{self.rpc_name_dict[parent_id]}\n➔ {self.rpc_name_dict[rpc_id]}', width=25) if parent_id != 65535 else self.wrap_label(f'{self.rpc_name_dict[rpc_id]}', width=25) for parent_id, rpc_id in df.index]
        # Create and return the plot
        return df.sort_values(ascending=False).head(5).hvplot.bar(title=title, xlabel='Remote Procedure Calls (RPC)', ylabel=ylabel, rot=0, height=500, width=1000).opts(shared_axes=False)

    """ Per-RPC Plots """
    def create_rpc_table_dataframe(self, src_files, dest_files):
        rpcs = []
        for index, row in self.origin_rpc_df.iterrows():
            file, address, name, rpc_id, provider_id, parent_rpc_id, parent_provider_id, sent_to = index  
            if address in src_files and sent_to in dest_files:
                RPC = f'{self.rpc_name_dict[parent_rpc_id]} ➔ {self.rpc_name_dict[rpc_id]}' if self.rpc_name_dict[parent_rpc_id] != 'None' else self.rpc_name_dict[rpc_id]
                rpcs.append({'Source': address, 'Target': sent_to, 'RPC': RPC})
        for index, row in self.target_rpc_df.iterrows():
            file, address, name, rpc_id, provider_id, parent_rpc_id, parent_provider_id, received_from = index    
            if received_from in src_files and address in dest_files:
                RPC = f'{self.rpc_name_dict[parent_rpc_id]} ➔ {self.rpc_name_dict[rpc_id]}' if self.rpc_name_dict[parent_rpc_id] != 'None' else self.rpc_name_dict[rpc_id]
                rpcs.append({'Source': received_from, 'Target': address, 'RPC': RPC})
        return pd.DataFrame(rpcs).drop_duplicates().reset_index(drop=True)

    @debug_time
    def create_graph_6(self, rpc_list):
        """
        Create a scatter plot with error bars showing top 5 server RPCs by average execution time,
        along with their min and max bounds.
        """
        # Step 1: Get filtered data using the modular function
        filtered_df = self._get_filtered_rpc_data(rpc_list, 'servers')
        # Step 2: Add RPC labels for grouping
        labels = []
        for _, row in filtered_df.iterrows():
            src_address = row[('meta', '', 'received_from')]
            dst_address = row[('meta', '', 'address')]
            parent_rpc_id = row[('meta', '', 'parent_rpc_id')]
            rpc_id = row[('meta', '', 'rpc_id')]
            src_name = self.rpc_name_dict[parent_rpc_id]
            dest_name = self.rpc_name_dict[rpc_id]
            label = f'{src_name} ➔ {dest_name}\n<{dst_address}>' if src_name != 'None' else f'{dest_name}\n<{dst_address}>'
            labels.append(label)
        filtered_df['rpc_label'] = labels
        # Step 3: Group by RPC label and calculate statistics
        grouped = filtered_df.groupby('rpc_label').agg({
            ('handler', 'duration', 'sum'): 'sum',
            ('handler', 'duration', 'max'): 'max', 
            ('handler', 'duration', 'min'): 'min',
            ('handler', 'duration', 'num'): 'sum'
        })
        # Filter out rows where call_count is 0 and build result data
        valid_data = grouped[grouped[('handler', 'duration', 'num')] > 0]
        data = []
        for rpc_label, row in valid_data.iterrows():
            time_sum = row[('handler', 'duration', 'sum')]
            time_max = row[('handler', 'duration', 'max')]
            time_min = row[('handler', 'duration', 'min')]
            call_count = row[('handler', 'duration', 'num')]
            time_avg = time_sum / call_count
            data.append({
                'RPC': rpc_label,
                'avg': time_avg,
                'min': time_min,
                'max': time_max,
                'neg_offset': time_avg - time_min,
                'pos_offset': time_max - time_avg,
            })
        if not data:
            raise ValueError("No data available: None of the selected RPCs were found in the server-side data. This may mean these RPCs were not executed on the server, were filtered out, or do not exist for your current selection.")
        # Step 4: Create visualization (unchanged)
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
    def create_graph_7(self, rpc_list):
        """
        Create a scatter plot with error bars showing top 5 client RPCs by average execution time,
        along with their min and max bounds.
        """
        # Step 1: Get filtered data using the modular function
        filtered_df = self._get_filtered_rpc_data(rpc_list, 'clients')
        # Step 2: Add RPC labels (unchanged)
        start = time.time()
        labels = []
        for _, row in filtered_df.iterrows():
            src_address = row[('meta', '', 'address')]
            dst_address = row[('meta', '', 'sent_to')]
            parent_rpc_id = row[('meta', '', 'parent_rpc_id')]
            rpc_id = row[('meta', '', 'rpc_id')]
            src_name = self.rpc_name_dict[parent_rpc_id]
            dest_name = self.rpc_name_dict[rpc_id]
            label = f'{src_name} ➔ {dest_name}\n<{src_address}>' if src_name != 'None' else f'{dest_name}\n<{src_address}>'
            labels.append(label)
        filtered_df['rpc_label'] = labels
        # Step 3: Group by RPC label and calculate statistics
        grouped = filtered_df.groupby('rpc_label').agg({
            ('iforward', 'duration', 'sum'): 'sum',
            ('iforward_wait', 'relative_timestamp_from_iforward_end', 'sum'): 'sum', 
            ('iforward_wait', 'duration', 'sum'): 'sum',
            ('iforward', 'duration', 'max'): 'max',
            ('iforward_wait', 'relative_timestamp_from_iforward_end', 'max'): 'max',
            ('iforward_wait', 'duration', 'max'): 'max',
            ('iforward', 'duration', 'min'): 'min',
            ('iforward_wait', 'relative_timestamp_from_iforward_end', 'min'): 'min',
            ('iforward_wait', 'duration', 'min'): 'min',
            ('iforward', 'duration', 'num'): 'sum'
        })
        # Convert grouped results back to the same data structure as before
        data = []
        for rpc_label, row in grouped.iterrows():
            time_sum = (row[('iforward', 'duration', 'sum')] + 
                    row[('iforward_wait', 'relative_timestamp_from_iforward_end', 'sum')] + 
                    row[('iforward_wait', 'duration', 'sum')])

            time_max = (row[('iforward', 'duration', 'max')] + 
                    row[('iforward_wait', 'relative_timestamp_from_iforward_end', 'max')] + 
                    row[('iforward_wait', 'duration', 'max')])

            time_min = (row[('iforward', 'duration', 'min')] + 
                    row[('iforward_wait', 'relative_timestamp_from_iforward_end', 'min')] + 
                    row[('iforward_wait', 'duration', 'min')])

            call_count = row[('iforward', 'duration', 'num')]
            if call_count > 0:
                time_avg = time_sum / call_count
                data.append({
                    'RPC': rpc_label,
                    'avg': time_avg,
                    'min': time_min,
                    'max': time_max,
                    'neg_offset': time_avg - time_min,
                    'pos_offset': time_max - time_avg,
                })
        if not data:
            raise ValueError("No data available: None of the selected RPCs were found in the client-side data. This may mean these RPCs were not issued by the client, were filtered out, or do not exist for your current selection.")
        # Step 4: Create visualization
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
    def create_graph_8(self, rpc_list, view_type):
        """
        Create a bar chart showing total time spent in each RPC step across all selected RPCs.
        
        This graph aggregates the total time spent in each step of the RPC process
        across all selected RPCs, helping identify bottlenecks.
        """
        if view_type == 'servers':
            functions = ['handler', 'ult', 'irespond', 'respond_cb', 'irespond_wait', 'set_output', 'get_input']
            color = '#ff7f0e'
            error_msg = "No data available: None of the selected RPCs were found in the server-side data. This may mean these RPCs were not executed on the server, were filtered out, or do not exist for your current selection."
        else:  # view_type == 'clients'
            functions = ['iforward', 'forward_cb', 'iforward_wait', 'set_input', 'get_output']
            color = '#1f77b4'
            error_msg = "No data available: None of the selected RPCs were found in the client-side data. This may mean these RPCs were not issued by the client, were filtered out, or do not exist for your current selection."
        # Step 1: Get filtered data using the modular function
        filtered_df = self._get_filtered_rpc_data(rpc_list, view_type)
        # Step 2: Calculate total duration for each function
        values = []
        for func in functions:
            try:
                total_duration = filtered_df[(func, 'duration', 'sum')].sum()
                values.append(total_duration)
            except KeyError:
                # Function might not exist in the data
                values.append(0)
        if sum(values) == 0:
            raise ValueError(error_msg)
        # Step 3: Create visualization
        plot_df = pd.DataFrame({
            'Function': functions, 
            'Total Duration': values, 
            'color': color
        }).sort_values(by='Total Duration', ascending=False)
        title = "Total Time Spent in Each RPC Step (Aggregated Across All Selected RPCs)"
        x_label = "RPC Step"
        y_label = "Total Duration (s, aggregated)"
        return plot_df.hvplot.bar(
            x='Function', 
            y='Total Duration', 
            title=title, 
            rot=45, 
            color='color', 
            xlabel=x_label, 
            ylabel=y_label, 
            height=500, 
            width=1000
        ).opts(shared_axes=False, default_tools=["pan"])

    @debug_time
    def create_graph_9(self, rpc_list):
        """
        Create a bar chart with error bars showing server-side RPC function statistics.
        
        This graph shows the mean duration and standard deviation for each server-side
        RPC function step, with error bars indicating the variability in execution times.
        """
        functions_server = ['handler', 'ult', 'irespond', 'respond_cb', 'irespond_wait', 'set_output', 'get_input']
        aggregations = ['duration', 'duration', 'duration', 'duration', 'duration', 'duration', 'duration']
        try:
            mean_server, var_server = self._get_rpc_mean_and_variance_per_step(rpc_list, 'server', functions_server, aggregations)
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
    def create_graph_10(self, rpc_list):
        """
        Create a bar chart with error bars showing client-side RPC function statistics.
        
        This graph shows the mean duration and standard deviation for each client-side
        RPC function step, with error bars indicating the variability in call times.
        """
        functions_client = ['iforward', 'forward_cb', 'iforward_wait', 'set_input', 'get_output']
        aggregations = ['duration', 'duration', 'duration', 'duration', 'duration']
        try:
            mean_client, var_client = self._get_rpc_mean_and_variance_per_step(rpc_list, 'client', functions_client, aggregations)
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
    def create_chord_graph(self, rpc_list, view_type='clients'):
        """
        Create a chord diagram showing RPC communication patterns between processes.
        
        This graph visualizes the flow of RPC calls between different processes,
        showing which processes communicate with each other and the volume of communication.
        """
        if view_type == 'clients':
            error_msg = "No data available: None of the selected RPCs were found in the client-side data. This may mean these RPCs were not issued by the client, were filtered out, or do not exist for your current selection."
        else:  # view_type == 'servers'
            error_msg = "No data available: None of the selected RPCs were found in the server-side data. This may mean these RPCs were not executed on the server, were filtered out, or do not exist for your current selection."
        # Step 1: Get filtered data using the modular function
        filtered_df = self._get_filtered_rpc_data(rpc_list, view_type)
        # Step 2: Build communication patterns and weights
        f = Counter()  # (src, dest): weight
        unique_nodes = set()
        for _, row in filtered_df.iterrows():
            if view_type == 'servers':
                # For servers: communication is from received_from to address
                address = row[('meta', '', 'address')]
                other_address = row[('meta', '', 'received_from')]
                src, dest = other_address, address
                try:
                    weight = row[('ult', 'duration', 'sum')]
                except KeyError:
                    weight = 0
            else:
                # For clients: communication is from address to sent_to
                address = row[('meta', '', 'address')]
                other_address = row[('meta', '', 'sent_to')]
                src, dest = address, other_address
                try:
                    weight = (row[('iforward', 'duration', 'sum')] + 
                            row[('iforward_wait', 'relative_timestamp_from_iforward_end', 'sum')] + 
                            row[('iforward_wait', 'duration', 'sum')])
                except KeyError:
                    weight = 0
            if weight > 0:  # Only add if there's actual communication
                f[(src, dest)] += weight
                unique_nodes.add(src)
                unique_nodes.add(dest)
        if not unique_nodes:
            raise ValueError(error_msg)
        # Step 3: Create node mapping
        address_to_node_id = {}
        for index, node in enumerate(list(unique_nodes)):
            address_to_node_id[node] = index
        # Step 4: Create edges
        edges = []
        for (u, v), w in f.items():
            edges.append({
                'source': address_to_node_id[u], 
                'target': address_to_node_id[v], 
                'weight': w
            })
        # Step 5: Create visualization
        chord_df = pd.DataFrame(edges)  
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
    def create_rpc_load_heatmap(self, rpc_list, view_type='clients'):
        """
        Create a heatmap showing RPC load distribution by clients or servers.
        
        This graph visualizes the distribution of RPC calls across different processes,
        showing which processes are handling which types of RPCs and the volume of each.
        """
        if view_type == 'clients':
            count_col = ('iforward', 'duration', 'num')
            error_msg = "No data available: None of the selected RPCs were found in the client-side data for the heatmap. This may mean these RPCs were not recorded, were filtered out, or do not exist for your current selection."
        else:  # view_type == 'servers'
            count_col = ('handler', 'duration', 'num')
            error_msg = "No data available: None of the selected RPCs were found in the server-side data for the heatmap. This may mean these RPCs were not recorded, were filtered out, or do not exist for your current selection."
        # Step 1: Get filtered data using the modular function
        filtered_df = self._get_filtered_rpc_data(rpc_list, view_type)
        # Step 2: Build the heatmap data directly without modifying the DataFrame
        rpcs = []
        for _, row in filtered_df.iterrows():
            rpc_id = row[('meta', '', 'rpc_id')]
            rpc_name = self.rpc_name_dict[rpc_id]
            address = row[('meta', '', 'address')]
            try:
                num_calls = row[count_col]
                if num_calls > 0:  # Only include if there are actual calls
                    rpcs.append({
                        'name': rpc_name,
                        'address': address, 
                        'num': num_calls
                    })
            except KeyError:
                # Column might not exist, skip this row
                continue
        if not rpcs:
            raise ValueError(error_msg)
        # Step 3: Create and process the DataFrame for heatmap
        df = pd.DataFrame(rpcs).set_index(['name', 'address']).groupby(['name', 'address']).sum()['num']        
        df_unstacked = df.unstack(fill_value=0)
        df_unstacked.index.name = 'RPC Type'
        df_unstacked.columns.name = f'{view_type.capitalize()}'
        # Sort rows (RPC types) by total activity descending
        row_sums = df_unstacked.sum(axis=1)
        df_unstacked = df_unstacked.loc[row_sums.sort_values(ascending=True).index]
        # Sort columns (addresses) by total activity descending
        col_sums = df_unstacked.sum(axis=0)
        df_unstacked = df_unstacked.loc[:, col_sums.sort_values(ascending=False).index]
        # Step 4: Create visualization 
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
    def create_per_rpc_svg_origin(self, rpc_list):
        """
        Create an SVG visualization of client-side RPC execution timeline.
        
        This function generates an SVG diagram showing the relative timing and duration
        of each step in the client-side RPC process, normalized to show the complete flow.
        """
        # Get mean and variance
        functions = ['iforward', 'forward_cb', 'iforward_wait', 'set_input', 'get_output']
        aggregations = ['duration', 'duration', 'duration', 'duration', 'duration']
        means, vars = self._get_rpc_mean_and_variance_per_step(rpc_list, 'client', functions, aggregations)
        iforward_duration = means[0]
        forward_cb_duration = means[1]
        wait_duration = means[2]
        set_input_duration = means[3]
        get_output_duration = means[4]
        functions = ['iforward', 'set_input', 'iforward_wait', 'forward_cb', 'get_output']
        aggregations = ['relative_timestamp_from_create', 'relative_timestamp_from_iforward_start', 'relative_timestamp_from_iforward_end', 'relative_timestamp_from_iforward_start', 'relative_timestamp_from_wait_end']
        means, vars = self._get_rpc_mean_and_variance_per_step(rpc_list, 'client', functions, aggregations)
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
    def create_per_rpc_svg_target(self, rpc_list):
        """
        Create an SVG visualization of server-side RPC execution timeline.
        
        This function generates an SVG diagram showing the relative timing and duration
        of each step in the server-side RPC process, normalized to show the complete flow.
        """
        # Get mean and variance
        functions = ['handler', 'ult', 'irespond', 'respond_cb', 'irespond_wait', 'set_output', 'get_input']
        aggregations = ['duration', 'duration', 'duration', 'duration', 'duration', 'duration', 'duration']
        means, vars = self._get_rpc_mean_and_variance_per_step(rpc_list, 'server', functions, aggregations)
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
        means, vars = self._get_rpc_mean_and_variance_per_step(rpc_list, 'server', functions, aggregations)
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
    
    def _get_filtered_rpc_data(self, rpc_list, view_type):
        """
        Common pattern for efficiently filtering RPC data using merge instead of multiple .xs() calls.
        """
        # Convert to tuple if needed for caching
        if isinstance(rpc_list, list):
            rpc_list = tuple(rpc_list)            
        return self._get_filtered_rpc_data_cached(rpc_list, view_type)

    @lru_cache(maxsize=128)
    def _get_filtered_rpc_data_cached(self, rpc_list, view_type):
        """
        Internal cached implementation of _get_filtered_rpc_data
        """
        if not rpc_list:
            raise ValueError("No RPCs provided")
        # Determine DataFrame and column mappings based on view_type
        if view_type == 'clients':
            ddf = self.stats.origin_rpc_ddf
            section_name = 'sent_to'
            address_col = 'address'
            error_msg = "No data available: None of the selected RPCs were found in the client-side data."
        elif view_type == 'servers':
            ddf = self.stats.target_rpc_ddf
            section_name = 'received_from'
            address_col = 'address'
            error_msg = "No data available: None of the selected RPCs were found in the server-side data."
        else:
            raise ValueError("view_type must be 'clients' or 'servers'")
        # Create filter DataFrame directly from rpc_list
        base_filter_data = []
        for src_address, dst_address, RPC in rpc_list:
            src, dest = self.get_src_dst_from_rpc_string(RPC)
            if src in self.rpc_id_dict and dest in self.rpc_id_dict:
                row_data = [
                    self.rpc_id_dict[src],  # parent_rpc_id
                    self.rpc_id_dict[dest], # rpc_id
                    src_address if view_type == 'servers' else dst_address,  # section_name address
                    dst_address if view_type == 'servers' else src_address,  # address
                ]
                base_filter_data.append(row_data)
        if not base_filter_data:
            raise ValueError("No valid RPCs found in rpc_id_dict")
        # Define base columns for merge
        base_columns = [
            ('meta', '', 'parent_rpc_id'),
            ('meta', '', 'rpc_id'),
            ('meta', '', section_name),
            ('meta', '', address_col)
        ]
        filter_df = pd.DataFrame(base_filter_data, columns=base_columns)
        # Ensure data types match
        filter_df[('meta', '', 'parent_rpc_id')] = filter_df[('meta', '', 'parent_rpc_id')].astype('uint64')
        filter_df[('meta', '', 'rpc_id')] = filter_df[('meta', '', 'rpc_id')].astype('uint64')
        # Ensure column structure matches exactly
        filter_df.columns = pd.MultiIndex.from_tuples(filter_df.columns)
        # Convert to dask DataFrame
        filter_ddf = dd.from_pandas(filter_df, npartitions=1)
        # Perform the merge
        filtered_ddf = ddf.merge(
            filter_ddf,
            on=base_columns,  # Always merge on the base columns
            how='inner'
        ).persist()
        # Compute once
        try:
            filtered_df = filtered_ddf.compute()
        except Exception as e:
            raise ValueError(f"Error computing filtered dataframe: {e}")
        if filtered_df.empty:
            raise ValueError(error_msg)
        return filtered_df

    def _get_rpc_mean_and_variance_per_step(self, rpc_list, type, functions, aggregations):
        """
        Get mean and variance statistics for RPC functions using the modular approach.
        """
        mean_result = [0] * len(functions)
        var_result = [0] * len(functions)
        # Use the modular function to get filtered data
        view_type = 'servers' if type == 'server' else 'clients'
        try:
            filtered_df = self._get_filtered_rpc_data(rpc_list, view_type)
        except ValueError as e:
            raise ValueError(str(e))
        # Create grouping key
        section_name = 'received_from' if type == 'server' else 'sent_to'
        filtered_df['rpc_key'] = (
            filtered_df[('meta', '', 'parent_rpc_id')].astype(str) + '_' +
            filtered_df[('meta', '', 'rpc_id')].astype(str) + '_' +
            filtered_df[('meta', '', section_name)] + '_' +
            filtered_df[('meta', '', 'address')]
        )
        # Process each function
        for func_idx, (func, agg) in enumerate(zip(functions, aggregations)):
            try:
                # Group by RPC key and aggregate by provider ID
                grouped = filtered_df.groupby('rpc_key').agg({
                    (func, agg, 'var'): 'max', # Let's get the max var so we can be conservative in our analysis
                    (func, agg, 'num'): 'sum',
                    (func, agg, 'avg'): 'max',
                    (func, agg, 'sum'): 'sum'
                })
                if grouped.empty:   
                    continue
                # Get valid data (where num > 0)
                valid_data = grouped[grouped[(func, agg, 'num')] > 0]
                if valid_data.empty:
                    continue
                # Calculate overall statistics
                total_num = valid_data[(func, agg, 'num')].sum()
                total_sum = valid_data[(func, agg, 'sum')].sum()
                total_mean = total_sum / total_num
                # Calculate variance using combining formula
                d_squared = (valid_data[(func, agg, 'avg')] - total_mean) ** 2
                total_variance = (valid_data[(func, agg, 'num')] * 
                                (valid_data[(func, agg, 'var')] + d_squared)).sum() / total_num
                mean_result[func_idx] = total_mean
                var_result[func_idx] = total_variance
            except KeyError as e:
                print(f"Warning: Function {func} with aggregation {agg} not found in data: {e}")
                continue
            except Exception as e:
                print(f"Error processing function {func}: {e}")
                continue
        if all(m is None for m in mean_result):
            raise ValueError("No data available: None of the selected RPCs were found in the server-side data.")
        return mean_result, var_result