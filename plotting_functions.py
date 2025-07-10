import holoviews as hv
import pandas as pd
import time

from collections import Counter
from holoviews import opts
from mochi_perf.graph import OriginRPCGraph, TargetRPCGraph

def debug_time(func):
    """Simple timing decorator"""
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        print(f"{func.__name__}: {time.time() - start:.2f}s")
        return result
    return wrapper

def get_src_dst_from_rpc_string(RPC):
    if '➔' in RPC:
        src, dest = RPC[:RPC.index('➔')-1], RPC[RPC.index('➔')+2:]
    else:
        src, dest = 'None', RPC
    return src, dest

def wrap_label(label, width=10):
    return '\n'.join([label[i:i+width] for i in range(0, len(label), width)])

def get_all_addresses(stats):
    address1 = stats.origin_rpc_df.index.get_level_values('address')
    address2 = stats.origin_rpc_df.index.get_level_values('sent_to')
    address3 = stats.target_rpc_df.index.get_level_values('address')
    address4 = stats.target_rpc_df.index.get_level_values('received_from')
    return sorted(address1.union(address2).union(address3).union(address4).unique().to_list())

""" 
Returns a tuple:
    (
        mean_client: list[float], the aggregated mean of all RPCs provided in <rpc_list> in each RPC step
        var_client: list[float], the aggregated variance of all RPCs provided in <rpc_list> in each RPC step
    )
"""
def get_mean_variance_from_rpcs_client(stats, rpc_id_dict, rpc_list, functions, aggregations):

    mean_client = [None] * len(functions)
    var_client = [None] * len(functions)

    num_rpc = [[] for _ in range(len(functions))]
    sum_rpc = [[] for _ in range(len(functions))]
    mean_rpc = [[] for _ in range(len(functions))]
    var_rpc = [[] for _ in range(len(functions))]

    for src_address, dst_address, RPC in rpc_list:
        src, dest = get_src_dst_from_rpc_string(RPC)

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

""" 
Returns a tuple:
    (
        mean_server: list[float], the aggregated mean of all RPCs provided in <rpc_list> in each RPC step
        var_server: list[float], the aggregated variance of all RPCs provided in <rpc_list> in each RPC step
    )
"""
def get_mean_variance_from_rpcs_server(stats, rpc_id_dict, rpc_list, functions, aggregations):

    mean_server = [None] * len(functions)
    var_server = [None] * len(functions)

    num_rpc = [[] for _ in range(len(functions))]
    sum_rpc = [[] for _ in range(len(functions))]
    mean_rpc = [[] for _ in range(len(functions))]
    var_rpc = [[] for _ in range(len(functions))]

    for src_address, dst_address, RPC in rpc_list:
        src, dest = get_src_dst_from_rpc_string(RPC)

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
def create_graph_1(stats):
    df = stats.origin_rpc_df

    step_1 = df['iforward']['duration']['sum'].rename('iforward_sum')
    step_2 = df['iforward_wait']['relative_timestamp_from_iforward_end']['sum'].rename('iforward_wait_rel_sum')
    step_3 = df['iforward_wait']['duration']['sum'].rename('iforward_wait_duration_sum')

    merged = pd.concat([step_1, step_2, step_3], axis=1)

    merged['total_sum'] = merged.sum(axis=1)

    aggregate_process_df = merged.groupby('address').agg('sum')

    return aggregate_process_df['total_sum'].sort_values(ascending=False).hvplot.bar(xlabel='Address', ylabel='Total Time', title='Total RPC Call Time by Process', rot=45, height=500, width=1000).opts(default_tools=["pan"], shared_axes=False)

@debug_time
def create_graph_2(stats):
    df = stats.target_rpc_df
    aggregate_process_df = df['ult']['duration']['sum'].groupby('address').agg('sum')

    return aggregate_process_df.sort_values(ascending=False).hvplot.bar(xlabel='Address', ylabel='Total Time', title='Total RPC Execution Time by Process', rot=45, height=500, width=1000).opts(default_tools=["pan"], shared_axes=False)

@debug_time
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

@debug_time
def create_graph_4(stats, address, rpc_name):   
    df = stats.target_rpc_df
    filtered_process = df.xs(address, level='address')

    new_x_labels = []
    for file, name, rpc_id, provider_id, parent_rpc_id, parent_provider_id, sent_to in filtered_process.index:
        new_x_labels.append(wrap_label(f'{rpc_name[parent_rpc_id]}\n➔ {rpc_name[rpc_id]}', width=25) if parent_rpc_id != 65535 else wrap_label(f'{rpc_name[rpc_id]}', width=25))

    filtered_process.index = new_x_labels

    return filtered_process['ult']['duration']['sum'].groupby(level=0).agg('sum').sort_values(ascending=False).head(5).hvplot.bar(xlabel='Remote Procedure Calls (RPC)', ylabel='Time', title=f'Top 5 RPC Execution Times for {address}', rot=0, height=500, width=1000).opts(default_tools=["pan"], shared_axes=False)

@debug_time
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

""" Per-RPC Plots """
@debug_time
def create_chord_graph(stats, rpc_id_dict, rpc_list, view_type='clients'):

    f = Counter() # (src, dest): weight -> where src and dest are processes, and weight is the total duration (depending on view_type)
    unique_nodes = set()

    for src_address, dst_address, RPC in rpc_list:
        src, dest = get_src_dst_from_rpc_string(RPC)
        
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
def create_graph_6(stats, rpc_id_dict, rpc_list):
    rpcs = []
    rpcs_index = []
    not_found = []
    for src_address, dst_address, RPC in rpc_list:
        src, dest = get_src_dst_from_rpc_string(RPC)

        try:
            df = stats.target_rpc_df.xs((rpc_id_dict[src], rpc_id_dict[dest], src_address, dst_address), level=[stats.target_rpc_df.index.names.index('parent_rpc_id'), stats.target_rpc_df.index.names.index('rpc_id'), stats.target_rpc_df.index.names.index('received_from'), stats.target_rpc_df.index.names.index('address')])
        except:
            not_found.append((src_address, dst_address, RPC)) 
            continue

        # file_name, provider_id and parent_provider_id are ignored when we aggregate everything up
        time_sum = df['handler']['duration']['sum'].sum()
        time_max = df['handler']['duration']['max'].max()
        time_min = df['handler']['duration']['min'].min()
        call_count = df['handler']['duration']['num'].sum()
        time_avg = time_sum / call_count

        rpcs_index.append(f'{src} \n➔ {dest}\n<{dst_address}>' if src != 'None' else f'{dest}\n<{dst_address}>')
        rpcs.append({'sum': time_sum, 'max': time_max, 'avg': time_avg, 'min': time_min, 'num': call_count})
    
    if rpcs:
        df = pd.DataFrame(rpcs).set_index([rpcs_index])
        return df[['max', 'avg', 'min']].sort_values(by='avg', ascending=False).head(5).hvplot.bar(
            height=500, 
            width=1000,
            title='Top 5 Server RPCs by Average Execution Time',
            xlabel='RPC Calls',
            ylabel='Execution Time (seconds)'
        ).opts(default_tools=["pan"], shared_axes=False)
    else:
        raise ValueError("No data available: None of the selected RPCs were found in the server-side data. This may mean these RPCs were not executed on the server, were filtered out, or do not exist for your current selection.")

@debug_time
def create_graph_7(stats, rpc_id_dict, rpc_list):
    rpcs = []
    rpcs_index = []
    not_found = []
    for src_address, dst_address, RPC in rpc_list:
        src, dest = get_src_dst_from_rpc_string(RPC)
        
        try:
            df = stats.origin_rpc_df.xs((rpc_id_dict[src], rpc_id_dict[dest], src_address, dst_address), level=[stats.origin_rpc_df.index.names.index('parent_rpc_id'), stats.origin_rpc_df.index.names.index('rpc_id'), stats.origin_rpc_df.index.names.index('address'), stats.origin_rpc_df.index.names.index('sent_to')])
        except:
            not_found.append((src_address, dst_address, RPC)) 
            continue
        
        # file_name, provider_id and parent_provider_id are ignored when we aggregate everything up
        time_sum = df['iforward']['duration']['sum'].sum() + df['iforward_wait']['relative_timestamp_from_iforward_end']['sum'].sum() + df['iforward_wait']['duration']['sum'].sum()
        time_max = df['iforward']['duration']['max'].max() + df['iforward_wait']['relative_timestamp_from_iforward_end']['max'].max() + df['iforward_wait']['duration']['max'].max()
        time_min = df['iforward']['duration']['min'].min() + df['iforward_wait']['relative_timestamp_from_iforward_end']['min'].min() + df['iforward_wait']['duration']['min'].min()
        call_count = df['iforward']['duration']['num'].sum()
        time_avg = time_sum / call_count

        rpcs_index.append(f'{src} \n➔ {dest}\n<{src_address}>' if src != 'None' else f'{dest}\n<{src_address}>')
        rpcs.append({'sum': time_sum, 'max': time_max, 'avg': time_avg, 'min': time_min, 'num': call_count})

    if rpcs:
        df = pd.DataFrame(rpcs).set_index([rpcs_index])
        return df[['max', 'avg', 'min']].sort_values(by='avg', ascending=False).head(5).hvplot.bar(
            height=500, 
            width=1000,
            title='Top 5 Client RPCs by Average Call Time',
            xlabel='RPC Calls',
            ylabel='Call Time (seconds)'
        ).opts(default_tools=["pan"], shared_axes=False)
    else:
        raise ValueError("No data available: None of the selected RPCs were found in the client-side data. This may mean these RPCs were not issued by the client, were filtered out, or do not exist for your current selection.")

@debug_time
def create_graph_8(stats, rpc_id_dict, rpc_list):

    functions_server = ['handler', 'ult', 'irespond', 'respond_cb', 'irespond_wait', 'set_output', 'get_input']
    functions_client = ['iforward', 'forward_cb', 'iforward_wait', 'set_input', 'get_output']
    values_server = [0] * len(functions_server)
    values_client = [0] * len(functions_client)
    
    for src_address, dst_address, RPC in rpc_list:
        src, dest = get_src_dst_from_rpc_string(RPC)

        try:
            # Get RPC server-side
            df = stats.target_rpc_df.xs((rpc_id_dict[src], rpc_id_dict[dest], src_address, dst_address), level=[stats.target_rpc_df.index.names.index('parent_rpc_id'), stats.target_rpc_df.index.names.index('rpc_id'), stats.target_rpc_df.index.names.index('received_from'), stats.target_rpc_df.index.names.index('address')])
            for index, func in enumerate(functions_server): # We are summing because there are still provider ID's
                values_server[index] += df[func]['duration']['sum'].sum()
        except:
            pass

        try:
            # Get RPC client-side
            df = stats.origin_rpc_df.xs((rpc_id_dict[src], rpc_id_dict[dest], src_address, dst_address), level=[stats.origin_rpc_df.index.names.index('parent_rpc_id'), stats.origin_rpc_df.index.names.index('rpc_id'), stats.origin_rpc_df.index.names.index('address'), stats.origin_rpc_df.index.names.index('sent_to')])
            for index, func in enumerate(functions_client):
                values_client[index] += df[func]['duration']['sum'].sum()
        except:
            pass

    origin_plot_df = pd.DataFrame({'Function': functions_client, 'Total Duration': values_client, 'color': '#1f77b4'}).sort_values(by='Total Duration', ascending=False)
    target_plot_df = pd.DataFrame({'Function': functions_server, 'Total Duration': values_server, 'color':'#ff7f0e'}).sort_values(by='Total Duration', ascending=False)

    title = "Total Time Spent in Each RPC Step (Aggregated Across All Selected RPCs)"
    x_label = "RPC Step"
    y_label = "Total Duration (s, aggregated)"

    if sum(values_client) and sum(values_server):
        return pd.concat([origin_plot_df, target_plot_df]).sort_values(by='Total Duration', ascending=False).hvplot.bar(x='Function', y='Total Duration', title=title, rot=45, color='color', xlabel=x_label, ylabel=y_label, height=500, width=1000).opts(shared_axes=False, default_tools=["pan"]) 
    elif sum(values_client):
        return origin_plot_df.hvplot.bar(x='Function', y='Total Duration', title=title, rot=45, color='color', xlabel=x_label, ylabel=y_label, height=500, width=1000).opts(shared_axes=False, default_tools=["pan"]) 
    elif sum(values_server):
        return target_plot_df.hvplot.bar(x='Function', y='Total Duration', title=title, rot=45, color='color', xlabel=x_label, ylabel=y_label, height=500, width=1000).opts(shared_axes=False, default_tools=["pan"]) 
    else:
        raise ValueError("No data available: None of the selected RPCs were found in either the server-side or client-side data. This may mean these RPCs were not executed, were filtered out, or do not exist for your current selection.")

@debug_time
def create_graph_9(stats, rpc_id_dict, rpc_list):

    functions_server = ['handler', 'ult', 'irespond', 'respond_cb', 'irespond_wait', 'set_output', 'get_input']
    aggregations = ['duration', 'duration', 'duration', 'duration', 'duration', 'duration', 'duration']
    try:
        mean_server, var_server = get_mean_variance_from_rpcs_server(stats, rpc_id_dict, rpc_list, functions_server, aggregations)
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

    return (bar_plot * error_plot).opts(shared_axes=False, default_tools=["pan"])

@debug_time
def create_graph_10(stats, rpc_id_dict, rpc_list):

    functions_client = ['iforward', 'forward_cb', 'iforward_wait', 'set_input', 'get_output']
    aggregations = ['duration', 'duration', 'duration', 'duration', 'duration']
    try:
        mean_client, var_client = get_mean_variance_from_rpcs_client(stats, rpc_id_dict, rpc_list, functions_client, aggregations)
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

    return (bar_plot * error_plot).opts(shared_axes=False, default_tools=["pan"])

@debug_time
def create_rpc_load_heatmap(stats, rpc_id_dict, rpc_list, view_type='clients'):
    rpcs = []
    for src_address, dst_address, RPC in rpc_list:
        src, dest = get_src_dst_from_rpc_string(RPC)
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
        xlabel=f'{wrap_label(view_type.capitalize())}',
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
def create_per_rpc_svg_origin(stats, rpc_id_dict, rpc_list):
    # Get mean and variance
    functions = ['iforward', 'forward_cb', 'iforward_wait', 'set_input', 'get_output']
    aggregations = ['duration', 'duration', 'duration', 'duration', 'duration']
    means, vars = get_mean_variance_from_rpcs_client(stats, rpc_id_dict, rpc_list, functions, aggregations)

    iforward_duration = means[0]
    forward_cb_duration = means[1]
    wait_duration = means[2]
    set_input_duration = means[3]
    get_output_duration = means[4]

    functions = ['iforward', 'set_input', 'iforward_wait', 'forward_cb', 'get_output']
    aggregations = ['relative_timestamp_from_create', 'relative_timestamp_from_iforward_start', 'relative_timestamp_from_iforward_end', 'relative_timestamp_from_iforward_start', 'relative_timestamp_from_wait_end']
    means, vars = get_mean_variance_from_rpcs_client(stats, rpc_id_dict, rpc_list, functions, aggregations)

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
def create_per_rpc_svg_target(stats, rpc_id_dict, rpc_list):
    # Get mean and variance
    functions = ['handler', 'ult', 'irespond', 'respond_cb', 'irespond_wait', 'set_output', 'get_input']
    aggregations = ['duration', 'duration', 'duration', 'duration', 'duration', 'duration', 'duration']
    means, vars = get_mean_variance_from_rpcs_server(stats, rpc_id_dict, rpc_list, functions, aggregations)

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
    means, vars = get_mean_variance_from_rpcs_server(stats, rpc_id_dict, rpc_list, functions, aggregations)

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
def get_heatmap_description(view_type):
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

def get_graph_1_description():
    return (
        "**What this shows:** See which processes are the most active clients in your system. "
        "Higher bars mean a process is making more RPC calls to others. Use this to spot your busiest clients."
    )

def get_graph_2_description():
    return (
        "**What this shows:** Find out which processes are doing the most work as servers. "
        "Higher bars mean a process is handling more RPC requests. This helps you spot overloaded servers."
    )

def get_graph_3_description():
    return (
        "**What this shows:** For the selected process, see how much time it spends calling each type of RPC. "
        "This helps you understand what kinds of work your client is doing most."
    )

def get_graph_4_description():
    return (
        "**What this shows:** For the selected process, see how much time it spends handling each type of incoming RPC. "
        "This reveals what kinds of requests your server is working on most."
    )

def get_graph_5_description():
    return (
        "**What this shows:** These are the top 5 RPCs using the most resources, based on your selected metric. "
        "Use this to quickly find which RPCs are slowing things down or using the most bandwidth."
    )

def get_graph_6_description():
    return (
        "**What this shows:** These are the top 5 server-side RPCs with the highest average execution time. "
        "For each, you can see the max, average, and min times. Use this to find slow server operations to optimize."
    )

def get_graph_7_description():
    return (
        "**What this shows:** These are the top 5 client-side RPCs with the highest average call time. "
        "For each, you can see the max, average, and min times. Use this to spot slow client operations or network delays."
    )

def get_graph_8_description():
    return (
        "**What this shows:** See how much total time is spent in each step of the RPC process, across all selected RPCs. "
        "Taller bars mean more time spent in that step. Focus on the tallest bars to find bottlenecks."
    )