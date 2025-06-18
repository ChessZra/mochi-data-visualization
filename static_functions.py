def format_time(seconds):
    if seconds < 1:
        return f"{seconds * 1000:.2f} ms"
    return f"{seconds:.2f} s"

def format_data_size(bytes):
    if bytes < 1024:
        return f"{bytes:.2f} B"
    elif bytes < 1024 * 1024:
        return f"{bytes/1024:.2f} KB"
    elif bytes < 1024 * 1024 * 1024:
        return f"{bytes/(1024*1024):.2f} MB"
    else:
        return f"{bytes/(1024*1024*1024):.2f} GB"

def wrap_label(label, width=10):
    return '\n'.join([label[i:i+width] for i in range(0, len(label), width)])

# The number of remote procedure calls (initiated by the origin process)
def get_number_of_rpc_calls(stats):
    return stats.origin_rpc_df['iforward']['duration']['num'].sum()

# Average execution Time (the mean of the average of RPC)
def get_average_execution_time(stats):
    return stats.target_rpc_df['ult']['duration']['avg'].mean()

# Max Execution Time
def get_max_execution_time(stats):
    return stats.target_rpc_df['ult']['duration']['max'].max()

# Total Data Transferred
def get_total_data_transferred(stats):
    return stats.bulk_transfer_df['itransfer']['size']['sum'].sum()

def get_source_df_given_callpath(stats, src, dest):
    return stats.origin_rpc_df[
        (stats.origin_rpc_df.index.get_level_values('parent_rpc_id') == src) & 
        (stats.origin_rpc_df.index.get_level_values('rpc_id') == dest)
    ]

def get_source_addresses_given_callpath(stats, src, dest):
    return get_source_df_given_callpath(stats, src, dest).index.get_level_values('address').unique().tolist()

def get_dest_df_given_callpath(stats, src, dest):
    return stats.target_rpc_df[
        (stats.target_rpc_df.index.get_level_values('parent_rpc_id') == src) & 
        (stats.target_rpc_df.index.get_level_values('rpc_id') == dest)
    ]

def get_dest_addresses_given_callpath(stats, src, dest):
    return get_dest_df_given_callpath(stats, src, dest).index.get_level_values('address').unique().tolist()