import holoviews as hv
import networkx as nx
import pandas as pd

from bokeh.models import HoverTool
from mochi_perf.graph import OriginRPCGraph, TargetRPCGraph
from static_functions import *

def create_graph_1(stats):
    df = stats.origin_rpc_df

    step_1 = df['iforward']['duration']['sum'].rename('iforward_sum')
    step_2 = df['iforward_wait']['relative_timestamp_from_iforward_end']['sum'].rename('iforward_wait_rel_sum')
    step_3 = df['iforward_wait']['duration']['sum'].rename('iforward_wait_duration_sum')

    merged = pd.concat([step_1, step_2, step_3], axis=1)

    merged['total_sum'] = merged.sum(axis=1)

    aggregate_process_df = merged.groupby('address').agg('sum')

    return aggregate_process_df['total_sum'].sort_values(ascending=False).hvplot.bar(xlabel='Address', ylabel='Total Time', title='Total RPC Call Time by Process', rot=0, height=500, width=1000).opts(default_tools=["pan"], shared_axes=False)

def create_graph_2(stats):
    df = stats.target_rpc_df
    aggregate_process_df = df['ult']['duration']['sum'].groupby('address').agg('sum')

    return aggregate_process_df.sort_values(ascending=False).hvplot.bar(xlabel='Address', ylabel='Total Time', title='Total RPC Execution Time by Process', rot=0, height=500, width=1000).opts(default_tools=["pan"], shared_axes=False)

def create_graph_3(stats, address, rpc_name):
  
    df = stats.origin_rpc_df
    filtered_process = df.xs(address, level='address')

    new_x_labels = []
    for file, name, rpc_id, provider_id, parent_rpc_id, parent_provider_id, sent_to in filtered_process.index:
        new_x_labels.append(wrap_label(f'{rpc_name[parent_rpc_id]}\n➔ {rpc_name[rpc_id]}', width=25) if parent_rpc_id != 65535 else wrap_label(f'{rpc_name[rpc_id]}', width=25))

    filtered_process.index = new_x_labels
    step_1 = filtered_process['iforward']['duration']['sum'].rename('iforward_sum')
    step_2 = filtered_process['iforward_wait']['relative_timestamp_from_iforward_end']['sum'].rename('iforward_wait_rel_sum')
    step_3 = filtered_process['iforward_wait']['duration']['sum'].rename('iforward_wait_duration_sum')

    merged = pd.concat([step_1, step_2, step_3], axis=1)

    merged['total_sum'] = merged.sum(axis=1)

    return merged['total_sum'].groupby(level=0).agg('sum').sort_values(ascending=False).head(5).hvplot.bar(xlabel='Remote Procedure Calls (RPC)', ylabel='Time', title=f'Top 5 RPC Call Times for {address}', rot=0, height=500, width=1000).opts(default_tools=["pan"], shared_axes=False)

def create_graph_4(stats, address, rpc_name):
    df = stats.target_rpc_df
    filtered_process = df.xs(address, level='address')

    new_x_labels = []
    for file, name, rpc_id, provider_id, parent_rpc_id, parent_provider_id, sent_to in filtered_process.index:
        new_x_labels.append(wrap_label(f'{rpc_name[parent_rpc_id]}\n➔ {rpc_name[rpc_id]}', width=25) if parent_rpc_id != 65535 else wrap_label(f'{rpc_name[rpc_id]}', width=25))

    filtered_process.index = new_x_labels

    return filtered_process['ult']['duration']['sum'].groupby(level=0).agg('sum').sort_values(ascending=False).head(5).hvplot.bar(xlabel='Remote Procedure Calls (RPC)', ylabel='Time', title=f'Top 5 RPC Execution Times for {address}', rot=0, height=500, width=1000).opts(default_tools=["pan"], shared_axes=False)

def create_graph_5(stats, metric, rpc_name):
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
    df.index = [wrap_label(f'{rpc_name[parent_id]}\n➔ {rpc_name[rpc_id]}', width=25) if parent_id != 65535 else wrap_label(f'{rpc_name[rpc_id]}', width=25) for parent_id, rpc_id in df.index]
    
    # Create and return the plot
    return df.sort_values(ascending=False).head(5).hvplot.bar(title=title, xlabel='Remote Procedure Calls (RPC)', ylabel=ylabel, rot=0, height=500, width=1000).opts(default_tools=["pan"], shared_axes=False)

def create_per_rpc_bar_plot(stats, src, dest, src_files, dest_files):
    if not src_files and not dest_files:
        return None

    # Get client data
    df = get_source_df_given_callpath(stats, src, dest)
    df = df[df.index.get_level_values('address').isin(src_files)]
    functions_client = ['iforward', 'forward_cb', 'iforward_wait', 'set_input', 'get_output']
    values_client = [df[func]['duration']['sum'].sum() for func in functions_client]
    
    # Get server data
    df = get_dest_df_given_callpath(stats, src, dest)
    df = df[df.index.get_level_values('address').isin(dest_files)]
    functions_server = ['handler', 'ult', 'irespond', 'respond_cb', 'irespond_wait', 'set_output', 'get_input']
    values_server = [df[func]['duration']['sum'].sum() for func in functions_server]

    # Combine server and client plots, then sort based on longest duration
    plot_df = pd.DataFrame({'Function': functions_client + functions_server, 'Total Duration': values_client + values_server}).sort_values(by='Total Duration', ascending=False)
    
    plot = plot_df.head(5).hvplot.bar(x='Function', y='Total Duration', title='Total Duration by Function', rot=45)
    plot.opts(default_tools=['hover'])
    plot.opts(shared_axes=False)
    return plot

def create_per_rpc_svg_origin(stats, src, dest, src_files):
    if not src_files:
        return None
    
    df = get_source_df_given_callpath(stats, src, dest)
    df = df[df.index.get_level_values('address').isin(src_files)]

    iforward_start = df['iforward']['relative_timestamp_from_create']['sum'].sum()
    set_input_start = iforward_start + df['set_input']['relative_timestamp_from_iforward_start']['sum'].sum()
    wait_start = iforward_start + df['iforward']['duration']['sum'].sum() + df['iforward_wait']['relative_timestamp_from_iforward_end']['sum'].sum()
    forward_cb_start = iforward_start + df['forward_cb']['relative_timestamp_from_iforward_start']['sum'].sum()
    get_output_start = wait_start + df['iforward_wait']['duration']['sum'].sum() + df['get_output']['relative_timestamp_from_wait_end']['sum'].sum()

    iforward_duration = df['iforward']['duration']['sum'].sum()
    set_input_duration = df['set_input']['duration']['sum'].sum()
    wait_duration = df['iforward_wait']['duration']['sum'].sum()
    forward_cb_duration = df['forward_cb']['duration']['sum'].sum()
    get_output_duration = df['get_output']['duration']['sum'].sum()

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

def create_per_rpc_svg_target(stats, src, dest, dest_files):
    if not dest_files:
        return None
    
    # Get client dataframe and groupby the address
    df = get_dest_df_given_callpath(stats, src, dest)
    df = df[df.index.get_level_values('address').isin(dest_files)]

    ult_start = df['ult']['relative_timestamp_from_handler_start']['sum'].sum()
    get_input_start = ult_start + df['get_input']['relative_timestamp_from_ult_start']['sum'].sum()
    irespond_start = ult_start + df['irespond']['relative_timestamp_from_ult_start']['sum'].sum()
    set_output_start = irespond_start + df['set_output']['relative_timestamp_from_irespond_start']['sum'].sum()
    wait_start = irespond_start + df['irespond']['duration']['sum'].sum() + df['irespond_wait']['relative_timestamp_from_irespond_end']['sum'].sum()
    respond_cb_start = irespond_start + df['respond_cb']['relative_timestamp_from_irespond_start']['sum'].sum()
    
    handler_duration = df['handler']['duration']['sum'].sum()
    ult_duration = df['ult']['duration']['sum'].sum()
    get_input_duration = df['get_input']['duration']['sum'].sum()
    irespond_duration = df['irespond']['duration']['sum'].sum()
    set_output_duration = df['set_output']['duration']['sum'].sum()
    wait_duration = df['irespond_wait']['duration']['sum'].sum()
    respond_cb_duration = df['respond_cb']['duration']['sum'].sum()

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

def create_rpc_load_heatmap(stats, view_type='clients'):
    if view_type == 'clients':
        df = stats.origin_rpc_df['iforward']['duration']['num'].groupby(['name', 'address']).sum()
    else:  
        df = stats.target_rpc_df['handler']['duration']['num'].groupby(['name', 'address']).sum()
        
    heatmap = df.unstack(fill_value=0).hvplot.heatmap(
        title=f'RPC Load Distribution by {view_type.capitalize()}',
        xlabel=f'{wrap_label(view_type.capitalize())}',
        ylabel='RPC Type',  
        cmap='viridis',
        rot=45,
        width=1000,
        height=500
    )    
    heatmap.opts(default_tools=['hover'])
    # Tricky setting: the axes are synchronized by default
    # this messes up with other plots!
    heatmap.opts(shared_axes=False, default_tools=["pan"]) 
    return heatmap

def create_communication_graph(stats):
    # Encode stats to a network

    # Define an inner class named Edge that will represent
    # an RPC call from a node (process) to another node
    class Edge:
        counter = 0
        def __init__(self, index):
            self.file = index[0]
            self.address = index[1] 
            self.rpc_name = index[2]
            self.rpc_id = index[3]
            self.provider_id = index[4]
            self.parent_rpc_id = index[5]
            self.parent_provider_id = index[6]
            # This corresponds to the field 'received from' or 'sent to' in the json file
            self.from_or_to = index[7]  

    G = nx.DiGraph()
    servers, clients = set(), set()
    origin_total_sent_df = stats.origin_rpc_df['iforward']['duration']['num'].groupby('address').sum()
    target_total_received_df = stats.target_rpc_df['handler']['duration']['num'].groupby('address').sum()

    for index, _ in stats.origin_rpc_df.iterrows():
        edge_details = Edge(index)        
        G.add_edge(edge_details.address, edge_details.from_or_to)
        servers.add(edge_details.from_or_to)
        clients.add(edge_details.address)
    for index, _ in stats.target_rpc_df.iterrows():
        edge_details = Edge(index)
        G.add_edge(edge_details.from_or_to, edge_details.address)
        servers.add(edge_details.address)
        clients.add(edge_details.from_or_to)

    # Set graph attributes
    node_roles = {}
    for node in G.nodes():
        if node in servers and node in clients:
            node_roles[node] = 'server/client'
        elif node in clients:
            node_roles[node] = 'client'
        elif node in servers:
            node_roles[node] = 'server'

    nx.set_node_attributes(G, {n: str(n) for n in G.nodes()}, 'label')
    nx.set_node_attributes(G, node_roles, 'role')
    nx.set_node_attributes(G, {n: target_total_received_df.get(n, 0) for n in G.nodes()}, 'RPC_dest')  
    nx.set_node_attributes(G, {n: origin_total_sent_df.get(n, 0) for n in G.nodes()}, 'RPC_src')  
    
    # Convert networkx graph to hv 
    hv_graph = hv.Graph.from_networkx(G, nx.circular_layout)
    for attr in ['role', 'RPC_dest', 'RPC_src']:
        hv_graph.nodes.data[attr] = hv_graph.nodes.data['index'].map(lambda n: G.nodes[n][attr])

    # Create a custom HoverTool with desired fields
    hover = HoverTool(tooltips=[("Role", "@role"), ("RPC_dest", "@RPC_dest"), ("RPC_src", "@RPC_src")])

    # Apply options to the graph including the custom hover tool and node color mapping
    hv_graph = hv_graph.opts(
        tools=[hover],
        node_color='role',
        cmap={'client': 'blue', 'server': 'green', 'server/client': 'orange'},
        width=1000,
        height=500,
    )

    # Add labels
    node_df = hv_graph.nodes.data.copy()
    node_df['y'] -= 0.075  # Shift labels slightly down
    labels = hv.Labels(node_df, ['x', 'y'], 'label').opts(text_font_size='14pt', text_color='black')

    # Combine
    return hv_graph * labels

def get_heatmap_description(view_type):
    if view_type == 'clients':
        return """Description: This heatmap visualizes how RPC load is distributed across different client processes. Each cell shows the number of RPC calls made by a specific client process for each RPC type, helping you identify which clients are the most active and which RPC types they use most frequently."""
    else: 
        return """Description: This heatmap visualizes how RPC load is distributed across different server processes. Each cell shows the number of RPC requests handled by a specific server process for each RPC type, helping you identify which servers are handling the most requests and which RPC types are most common."""

def get_graph_1_description():
    return """Description: This chart displays the total time each process spent making RPC calls to other processes. It shows the client-side perspective of RPC communication, revealing which processes are the most active clients."""

def get_graph_2_description():
    return """Description: This chart shows the cumulative time each process spent executing RPC requests. The height of each bar represents the total execution time for that process, helping you identify which processes are doing the most computational work."""

def get_graph_3_description():
    return """Description: This detailed view shows how much time the selected process spent calling each specific RPC type. It helps you understand the client-side behavior of a particular process."""

def get_graph_4_description():
    return """Description: This chart shows how much time the selected process spent executing each type of RPC request it received. It reveals the server-side workload distribution.
    """

def get_graph_5_description():
    return """Description: This chart displays the top 5 most resource-intensive Remote Procedure Calls (RPCs) in your system, ranked by the selected performance metric. It helps you quickly identify which RPC operations are consuming the most resources, whether that's server execution time, client call time, bulk transfer time, or data volume."""

