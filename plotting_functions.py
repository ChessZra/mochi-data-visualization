import holoviews as hv
import networkx as nx
import pandas as pd
import panel as pn

from bokeh.models import HoverTool
from mochi_perf.graph import OriginRPCGraph, TargetRPCGraph
from static_functions import *

class Edge:
    # Edges represent the RPC
    counter = 0
    def __init__(self, index):
        self.file = index[0]
        self.address = index[1] 
        self.rpc_name = index[2]
        self.rpc_id = index[3]
        self.provider_id = index[4]
        self.parent_rpc_id = index[5]
        self.parent_provider_id = index[6]
        self.fromto = index[7] # Depends 

def create_node_graph(stats):

    # ==================== Build graph ====================
    G = nx.DiGraph()
    servers, clients = set(), set()
    origin_total_sent_df = stats.origin_rpc_df['iforward']['duration']['num'].groupby('address').sum()
    target_total_received_df = stats.target_rpc_df['handler']['duration']['num'].groupby('address').sum()

    for index, _ in stats.origin_rpc_df.iterrows():
        edge_details = Edge(index)        
        G.add_edge(edge_details.address, edge_details.fromto)
        servers.add(edge_details.fromto)
        clients.add(edge_details.address)
    for index, _ in stats.target_rpc_df.iterrows():
        edge_details = Edge(index)
        G.add_edge(edge_details.fromto, edge_details.address)
        servers.add(edge_details.address)
        clients.add(edge_details.fromto)

    # ==================== Set graph attributes ====================
    role_cmap = {'client': 'blue', 'server': 'green', 'server/client': 'orange'}
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
    
    # ==================== Networkx graph to hv ====================
    hv_graph = hv.Graph.from_networkx(G, nx.circular_layout)
    for attr in ['role', 'RPC_dest', 'RPC_src']:
        hv_graph.nodes.data[attr] = hv_graph.nodes.data['index'].map(lambda n: G.nodes[n][attr])

    # Create a custom HoverTool with desired fields
    hover = HoverTool(
        tooltips=[
            ("Role", "@role"),
            ("RPC_dest", "@RPC_dest"),
            ("RPC_src", "@RPC_src")
        ],
        point_policy="follow_mouse"
    )

    # Apply options to the graph including the custom hover tool and node color mapping
    hv_graph = hv_graph.opts(
        tools=[hover],
        inspection_policy='nodes',
        node_color='role',
        cmap=role_cmap,
        width=800,
        height=400,
        legend_position='right',
    )

    # ==================== Add labels ====================
    node_df = hv_graph.nodes.data.copy()
    node_df['y'] -= 0.075  # Shift labels slightly down
    labels = hv.Labels(node_df, ['x', 'y'], 'label').opts(
        text_font_size='14pt', text_color='black')

    # ==================== Combine ====================
    return hv_graph * labels

def create_main_plot(stats, metric, aggregation, rpc_name):
    
    # Determine which dataframe to use based on the metric
    if metric == 'RPC Execution Time':
        # Server-side execution time
        df = stats.target_rpc_df["ult"]["duration"][aggregation]
        title = f'{aggregation.capitalize()} time spent by servers executing each RPC'
        if aggregation == 'num':
            ylabel = 'Count (number of calls)'
        elif aggregation == 'var':
            ylabel = 'Time² (in seconds²)'
        else:
            ylabel = 'Time (in seconds)'
        
    elif metric == 'Client Call Time':
        # Client-side call time
        df = (stats.origin_rpc_df['iforward']['duration'][aggregation] + 
              stats.origin_rpc_df['iforward_wait']['relative_timestamp_from_iforward_end'][aggregation] + 
              stats.origin_rpc_df['iforward_wait']['duration'][aggregation])
        title = f'{aggregation.capitalize()} time spent by clients calling this RPC'
        if aggregation == 'num':
            # Divide it by three because we added iforward + iforward_wait_relative + iforward_wait_end
            df /= 3 
            ylabel = 'Count (number of calls)'
        elif aggregation == 'var':
            ylabel = 'Time² (in seconds²)'
        else:
            ylabel = 'Time (in seconds)'

    elif metric == 'Bulk Transfer Time':
        # Bulk transfer time
        df = (stats.bulk_transfer_df['itransfer']['duration'][aggregation] + 
              stats.bulk_transfer_df['itransfer_wait']['relative_timestamp_from_itransfer_end'][aggregation] + 
              stats.bulk_transfer_df['itransfer_wait']['duration'][aggregation])
        title = f'{aggregation.capitalize()} bulk transfer time for this RPC'
        if aggregation == 'num':
            # Divide it by three because we added itransfer + itransfer_wait + itransfer_wait_end
            df /= 3
            ylabel = 'Count (number of calls)'
        elif aggregation == 'var':
            ylabel = 'Time² (in seconds²)'
        else:
            ylabel = 'Time (in seconds)'
        
    elif metric == 'RDMA Data Transfer Size':
        # RDMA transfer size
        df = stats.bulk_transfer_df['itransfer']['size'][aggregation]
        title = f'{aggregation.capitalize()} amount of data transferred using RDMA from this RPC'
        if aggregation == 'num':
            ylabel = 'Count (number of calls)'
        elif aggregation == 'var':
            ylabel = 'Time² (in seconds²)'
        else:
            ylabel = 'Time (in seconds)'

    else:
        raise Exception('Exception, invalid metric passed')
    
    # Group the RPCs by it's RPC path (parent -> child)
    # Different files will combine with each other
    if aggregation in ['sum', 'num']:
        df = df.groupby(["parent_rpc_id", "rpc_id"]).sum()
    elif aggregation in ['avg', 'var']:
        df = df.groupby(["parent_rpc_id", "rpc_id"]).mean() 
    elif aggregation == 'max':
        df = df.groupby(["parent_rpc_id", "rpc_id"]).max()
    elif aggregation == 'min':
        df = df.groupby(["parent_rpc_id", "rpc_id"]).min()

    new_index = []
    for parent_id, rpc_id in df.index:
        new_index.append(f'{rpc_name[parent_id]}\n➔ {rpc_name[rpc_id]}' if parent_id != 65535 else f'{wrap_label(rpc_name[rpc_id])}')

    df.index = new_index
    df = df.sort_values(ascending=False)

    # Create and return the plot
    plot = df.head(5).hvplot.bar(
        title=title,
        xlabel='Remote Procedure Calls (RPC)',
        ylabel=ylabel,
        rot=0,
        height=400,
        width=800,
        fontsize={'title': 18, 'labels': 16, 'xticks': 14, 'yticks': 14},
    )
    plot.opts(default_tools=['hover'])
    plot.opts(shared_axes=False)
    return plot

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
    
    plot = plot_df.head(5).hvplot.bar(x='Function', y='Total Duration', title='Total Duration by Function')
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
        xlabel=f'{view_type.capitalize()}',
        ylabel='RPC Type',  
        cmap='viridis',
        width=800,
        height=400
    )    
    heatmap.opts(default_tools=['hover'])
    # Tricky setting: the axes are synchronized by default
    # this messes up with other plots!
    heatmap.opts(shared_axes=False) 
    return heatmap

