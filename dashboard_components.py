import panel as pn

import holoviews as hv
import hvplot.pandas

from holoviews.streams import Tap
from plotting_functions import *
from static_functions import *

# Enhanced styling for better UX
title_style = {
    'font-size': '28px',
    'font-weight': 'bold',
    'color': '#2c3e50',
    'margin-bottom': '20px',
    'text-align': 'center'
}

section_style = {
    'font-size': '22px',
    'font-weight': '600',
    'color': '#34495e',
    'margin-bottom': '15px',
    'padding-bottom': '10px',
    'border-bottom': '2px solid #ecf0f1'
}

sub_section_style = {
    'font-size': '18px',
    'font-weight': '500',
    'color': '#34495e',
    "white-space": "pre-wrap", 
    "overflow-wrap": "break-word",
    "max-width": "1000px", 
    'margin-bottom': '10px'
}

description_style = {
    "font-size": "15px",
    "color": "#555",
    "line-height": "1.6",
    "white-space": "pre-wrap", 
    "overflow-wrap": "break-word",
    "max-width": "1000px", 
    'margin-bottom': '15px'
}

border_style = {
    'background': '#ffffff',
    'padding': '25px',
    'margin': '15px 0',
    'border-radius': '12px',
    'box-shadow': '0 4px 6px rgba(0,0,0,0.1)',
    'border-left': '4px solid #3498db'
}

highlight_style = {
    'background': '#f8f9fa',
    'padding': '20px',
    'margin': '15px 0',
    'border-radius': '8px',
    'border-left': '4px solid #e74c3c',
    'font-size': '14px',
    "max-width": "1000px",
    'color': '#2c3e50'
}

tip_style = {
    'background': '#e8f4fd',
    'padding': '15px',
    'margin': '10px 0',
    'border-radius': '8px',
    'border-left': '4px solid #3498db',
    'font-size': '14px',
    "max-width": "1000px",
    'color': '#2c3e50'
}

side_note_style = {
    'font-style': 'italic', 
    'color': '#7f8c8d', 
    'font-size': '13px',
    'background': '#f8f9fa',
    'padding': '10px',
    'border-radius': '6px',
    'margin': '10px 0'
}
main_page_width = 1100
pn.extension('tabulator')

class MochiDashboard():
    def __init__(self, stats):
        self.rpc_name_dict = {65535: 'None'}
        self.rpc_id_dict = {'None': 65535}

        for df in [stats.origin_rpc_df, stats.target_rpc_df]:
            for index in df.index:
                self.rpc_name_dict[index[3]], self.rpc_id_dict[index[2]] = index[2], index[3]

        # Create main dashboard with improved flow
        main_page = pn.Column(
            self._create_header(),
            self._create_overview_section(stats),
            self._create_process_analysis_section(stats),
            self._create_detailed_analysis_section(stats),
            styles={'max-width': '1200px', 'margin': '0 auto'}
        )
        
        # Navigation system
        self.trigger = pn.widgets.TextInput(name='Page Toggle', value='', visible=False)
        @pn.depends(self.trigger)
        def get_page(context):
            print("get_page() called with context:", context)
            if not context or context == 'back_to_main_page':
                return main_page
            else:
                return self._create_per_rpc_view(context, stats)
        
        template = pn.template.MaterialTemplate(
            title="Mochi Performance Dashboard", 
            header_background="#2c3e50",
            main=get_page
        )
        template.show()

    def _create_header(self):
        """Create an engaging header with clear purpose"""
        return pn.Column(
            pn.pane.Markdown(
                "# Mochi Performance Dashboard",
                styles=title_style
            ),
            pn.pane.Markdown(
                "**Welcome!** This dashboard helps you understand how your distributed system is performing. "
                "Start with the overview to get the big picture, then dive deeper into specific processes and RPC calls.",
                styles=description_style
            ),
            pn.pane.Markdown(
                "**Quick Start:** Scroll down to see system-wide performance, then select a process to analyze its behavior, "
                "and finally click on any RPC bar to explore detailed statistics.",
                styles=tip_style
            ),
            styles=border_style,
            width=main_page_width
        )

    def _create_overview_section(self, stats):
        """Create high-level system overview with user-friendly explanations"""
        return pn.Column(
            pn.pane.Markdown("## System Overview", styles=section_style),
            pn.pane.Markdown(
                "Let's start with the big picture. These charts show you which processes are doing the most work "
                "and how your system is communicating overall.",
                styles=description_style
            ),
            pn.Column(
                create_graph_1(stats),
                pn.pane.Markdown(
                    get_graph_1_description(),
                    styles=description_style
                ),
            ),
            pn.Column(
                create_graph_2(stats),
                pn.pane.Markdown(
                    get_graph_2_description(),
                    styles=description_style
                ),
            ),
            pn.pane.Markdown(
                "**Look for:** Processes with unusually high bars - they might be bottlenecks or need optimization.",
                styles=side_note_style
            ),
            styles=border_style,
            width=main_page_width
        )

    def _create_process_analysis_section(self, stats):
        """Create interactive process analysis with guided exploration"""
        process_dropdown = pn.widgets.Select(
            name='Select a Process to Analyze', 
            options=get_all_addresses(stats),
            width=400
        )

        @pn.depends(process_dropdown)
        def get_process_analysis(process_choice):
            if not process_choice:
                return pn.pane.Markdown(
                    "**Select a process above to see its detailed behavior**",
                    styles=description_style
                )
            
            try:
                client_view = create_graph_3(stats, process_choice, self.rpc_name_dict)
            except:
                client_view = pn.pane.Markdown(
                    f"**No data available for {process_choice}** - This process may not have made any RPC calls.",
                    styles=highlight_style
                )
            
            try:
                server_view = create_graph_4(stats, process_choice, self.rpc_name_dict)
            except:
                server_view = pn.pane.Markdown(
                    f"**No data available for {process_choice}** - This process may not have handled any RPC calls.",
                    styles=highlight_style
                )

            return pn.Column(
                pn.Column(
                    pn.pane.Markdown("**As a Client** (RPCs it calls)", styles=sub_section_style),
                    client_view,
                    pn.pane.Markdown(
                        get_graph_3_description(),
                        styles=description_style
                    ),
                ),
                pn.Column(
                    pn.pane.Markdown("**As a Server** (RPCs it handles)", styles=sub_section_style),
                    server_view,
                    pn.pane.Markdown(
                        get_graph_4_description(),
                        styles=description_style
                    ),
                )
            )
        
        return pn.Column(
            pn.pane.Markdown("## Process Deep Dive", styles=section_style),
            pn.pane.Markdown(
                "Now let's look at individual processes. Select one to see how it behaves as both a client "
                "(making calls) and a server (handling requests).",
                styles=description_style
            ),
            process_dropdown,
            get_process_analysis,
            styles=border_style,
            width=main_page_width
        )

    def _create_detailed_analysis_section(self, stats):
        """Create the RPC analysis section with clear call-to-action"""
        metric_dropdown = pn.widgets.Select(
            name='Performance Metric', 
            options=['Server Execution Time', 'Client Call Time', 'Bulk Transfer Time', 'RDMA Data Transfer Size'], 
            value='Server Execution Time',
            width=300
        )

        @pn.depends(metric_dropdown)
        def get_visualization(metric_choice):
            bars = create_graph_5(stats, metric_choice, self.rpc_name_dict)
        
            # Bar click callback
            tap = Tap(source=bars)
            @pn.depends(tap.param.x)
            def on_bar_click(x):
                if x is not None:
                    self.trigger.value = x
                return ''
            
            return pn.Column(
                bars,
                on_bar_click,
                pn.pane.Markdown(
                    get_graph_5_description() + " Click any bar to explore detailed statistics for that specific RPC call.",
                    styles=description_style
                )
            )
        
        return pn.Column(
            pn.pane.Markdown("## RPC Performance Analysis", styles=section_style),
            pn.pane.Markdown(
                "Ready to dive deeper? This section shows you the most resource-intensive RPC calls in your system. "
                "Choose a metric to focus on, then click any bar to see detailed breakdowns.",
                styles=description_style
            ),
                pn.Row(metric_dropdown),
                get_visualization,
            pn.pane.Markdown(
                "**Pro Tip:** Start with 'Server Execution Time' to find the slowest operations, "
                "then explore 'Client Call Time' to see if delays are in the network or processing.",
                styles=tip_style
            ),
            styles=border_style,
            width=main_page_width
        )

    def _create_per_rpc_view(self, context, stats):
        """Create the detailed RPC view with improved user guidance"""
        # Define components
        all_options = get_all_addresses(stats)
        origin_select = pn.widgets.MultiSelect(
            name='Source Processes', 
            options=all_options, 
            size=8, 
            width=300
        )
        target_select = pn.widgets.MultiSelect(
            name='Destination Processes', 
            options=all_options, 
            size=8, 
            width=300
        )
        apply_button = pn.widgets.Button(
            name='Apply', 
            button_type='primary',
            width=150
        )
        back_button = pn.widgets.Button(
            name='← Back to Overview', 
            button_type='default',
            width=150
        )
        
        rpc_table_wrapper = pn.Column()
        right_layout = pn.Column()
        self.src_files, self.dest_files = [], []
        self.tab_selection = []

        # Callbacks
        def on_back_button_click(event):
            self.trigger.value = 'back_to_main_page'

        def create_rpc_dataframe():
            rpcs = []
            for index, row in stats.origin_rpc_df.iterrows():
                file, address, name, rpc_id, provider_id, parent_rpc_id, parent_provider_id, sent_to = index  
                
                if address in self.src_files and sent_to in self.dest_files:
                    RPC = f'{self.rpc_name_dict[parent_rpc_id]} ➔ {self.rpc_name_dict[rpc_id]}' if self.rpc_name_dict[parent_rpc_id] != 'None' else self.rpc_name_dict[rpc_id]
                    rpcs.append({'Source': address, 'Target': sent_to, 'RPC': RPC})

            for index, row in stats.target_rpc_df.iterrows():
                file, address, name, rpc_id, provider_id, parent_rpc_id, parent_provider_id, received_from = index    
                if received_from in self.src_files and address in self.dest_files:
                    RPC = f'{self.rpc_name_dict[parent_rpc_id]} ➔ {self.rpc_name_dict[rpc_id]}' if self.rpc_name_dict[parent_rpc_id] != 'None' else self.rpc_name_dict[rpc_id]
                    rpcs.append({'Source': received_from, 'Target': address, 'RPC': RPC})
            
            return pd.DataFrame(rpcs).drop_duplicates().reset_index(drop=True)

        def origin_on_change(event):
            self.src_files = event.new

        def target_on_change(event):
            self.dest_files = event.new

        def tabulator_selection_on_change(event):
            self.tab_selection = event.new

        def on_tabulation_confirm_view_click(event):
            if not self.tab_selection:
                return
            
            rpc_list = []
            for row_index in self.tab_selection:
                src_address, dst_address = rpc_table_wrapper[0][2].value.iloc[row_index]['Source'], rpc_table_wrapper[0][2].value.iloc[row_index]['Target']
                RPC = rpc_table_wrapper[0][2].value.loc[row_index]['RPC']
                rpc_list.append((src_address, dst_address, RPC))
            
            right_layout.clear()
            right_layout.append(self._create_detailed_per_rpc_layout(stats, rpc_list))

        def on_apply_button_click(event):
            if not self.src_files or not self.dest_files:
                return
                        
            # Create table with better styling
            df = create_rpc_dataframe()
            table = pn.widgets.Tabulator(df, selectable=True, disabled=True, configuration={'selectableRowsRangeMode': 'click'})
            
            confirm_button = pn.widgets.Button(
                name='View Detailed Analysis', 
                button_type='primary',
                width=200
            )
            
            rpc_table_wrapper.clear()
            rpc_table_wrapper.append(
                pn.Column(
                    pn.pane.Markdown("### Step 2: Select RPCs to Analyze", styles=sub_section_style),
                    pn.pane.Markdown(
                        f"Found **{len(df)}** RPC communication(s) between your selected processes. Choose which ones you want to analyze in detail: ",
                        styles=description_style
                    ),
                    table,
                    pn.pane.Markdown(
                        "**Selection Tips:** Click to select individual rows, Ctrl+click for multiple selections, or Shift+click for ranges",
                        styles=tip_style
                    ),
                    confirm_button,
                    pn.pane.Markdown('*Note: You may notice provider IDs are included in the logs. In our case, RPC grouping ignores provider ID and parent provider ID for simplified analysis*',styles=side_note_style),  
                    styles=border_style,
                    width=700
                )
            )
            
            # Wire up callbacks
            table.param.watch(tabulator_selection_on_change, 'selection')
            confirm_button.on_click(on_tabulation_confirm_view_click)

        # Wire up callbacks
        origin_select.param.watch(origin_on_change, 'value')
        target_select.param.watch(target_on_change, 'value')
        apply_button.on_click(on_apply_button_click)
        back_button.on_click(on_back_button_click)

        # Main layout
        return pn.Row(
            pn.Column(
                pn.Column(
                    pn.pane.Markdown(f"## PER-RPC Statistics Page", styles=section_style),
                    pn.pane.Markdown(
                        "You're now analyzing a specific RPC type. Select the source and destination processes "
                        "you want to examine, then choose which specific RPC calls to analyze in detail.",
                        styles=description_style
                    ),
                    pn.pane.Markdown("### Step 1: Choose Your Processes", styles=sub_section_style),
                    pn.Row(
                        pn.pane.Markdown("• **Source**: Processes that initiate RPC calls (clients)\n• **Destination**: Processes that receive and handle RPC calls (servers)", styles=description_style),
                        apply_button    
                    ),
                    pn.Row(origin_select, target_select),
                    styles=border_style,
                    width=700
                ),
                rpc_table_wrapper,
            ),
            pn.Column(
                right_layout,
                pn.Row(back_button, styles={'margin-top': '20px'}),
                styles={'max-width': '1400px', 'margin': '0 auto'}
            ),
        )

    def _create_detailed_per_rpc_layout(self, stats, rpc_list):

        try:
            graph_6_view = create_graph_6(stats, self.rpc_id_dict, rpc_list)
        except Exception as e:
            graph_6_view = pn.pane.Markdown(
                f"{str(e)}",
                styles=highlight_style
            )
        try:
            graph_7_view = create_graph_7(stats, self.rpc_id_dict, rpc_list)
        except Exception as e:
            graph_7_view = pn.pane.Markdown(
                f"{str(e)}",
                styles=highlight_style
            )
        try:
            graph_8_view = create_graph_8(stats, self.rpc_id_dict, rpc_list)
        except Exception as e:
            graph_8_view = pn.pane.Markdown(
                f"{str(e)}",
                styles=highlight_style
            )
        try:
            graph_9_view = create_graph_9(stats, self.rpc_id_dict, rpc_list)
        except Exception as e:
            graph_9_view = pn.pane.Markdown(
                f"{str(e)}",
                styles=highlight_style
            )
        try:
            graph_10_view = create_graph_10(stats, self.rpc_id_dict, rpc_list)
        except Exception as e:
            graph_10_view = pn.pane.Markdown(
                f"{str(e)}",
                styles=highlight_style
            )

        """Create the detailed analysis layout with user-friendly explanations"""
        return pn.Column(
            pn.pane.Markdown("## Detailed Performance Analysis", styles=section_style),
            pn.pane.Markdown(
                f"Analyzing **{len(rpc_list)} RPC calls**. Here's what we found:",
                styles=description_style
            ),
            
            pn.pane.Markdown("### Timing Breakdown", styles=sub_section_style),
            pn.pane.Markdown(
                "Let's start by looking at the performance of your selected RPCs. Which ones are taking the most time to execute?",
                styles=description_style
            ),     
            pn.Column(
                graph_6_view,
                pn.pane.Markdown(
                    get_graph_6_description(),
                    styles=description_style
                ),
            ),
            pn.Column(
                graph_7_view,
                pn.pane.Markdown(
                    get_graph_7_description(),
                    styles=description_style
                ),
            ),
            pn.pane.Markdown("### Workload Distribution", styles=sub_section_style),
            pn.pane.Markdown(
                "How is the work distributed across your processes? This helps you understand load balancing and identify potential bottlenecks.",
                styles=description_style
            ),
            self._create_distribution_view(stats, rpc_list),
            
            pn.pane.Markdown("### Understanding RPC Lifecycle", styles=sub_section_style),
            pn.pane.Markdown(
                "RPCs go through several stages from initiation to completion. The diagrams below show the typical flow:",
                styles=description_style
            ),
            pn.Row(
                pn.Column(
                    pn.pane.Markdown(
                        "**Client Side (Left):** Initiates the RPC call and waits for response",
                        width=300,
                        styles=description_style
                    ),
                    pn.pane.Markdown(
                        "**Server Side (Right):** Receives the request, processes it, and sends back results",
                        width=300,
                        styles=description_style
                    ),
                    pn.pane.Markdown(
                        "This flow helps you understand where time is being spent in your system.",
                        width=300,
                        styles=description_style
                    ),
                ),
                pn.pane.SVG("./img/rpc-origin.svg", width=300, height=400),
                pn.pane.SVG("./img/rpc-target.svg", width=300, height=400),
            ),
            
            pn.pane.Markdown("### Where is Time Being Spent?", styles=sub_section_style),
            pn.pane.Markdown(
                "This breakdown shows exactly where time is being consumed in your RPC workflow. Look for the tallest bars - those are your bottlenecks!",
                styles=description_style
            ),
            pn.pane.Markdown(
                "**Legend:** <span style='color:#1f77b4'>🔵 Client steps</span> &nbsp;&nbsp; <span style='color:#ff7f0e'>🟠 Server steps</span>", 
                styles=description_style
            ),
            graph_8_view,
            pn.pane.Markdown(
                get_graph_8_description(),
                styles=description_style
            ),

            pn.pane.Markdown("### Performance Variability", styles=sub_section_style),
            pn.pane.Markdown(
                "These charts show which functions have the most unpredictable performance. "
                "High variability (large error bars) might indicate issues that need attention.",
                styles=description_style
            ),
            graph_9_view,
            graph_10_view,
            
            pn.pane.Markdown(
                "**Analysis Tips:** Look for patterns in the data. Are certain steps consistently slow? "
                "Do some RPCs show much more variability than others? These insights can guide your optimization efforts.",
                styles=tip_style
            ),
            styles=border_style
        ) 
   
    def _create_distribution_view(self, stats, rpc_list):
        """Create distribution view with user-friendly explanations"""
        try:
            client_heatmap = create_rpc_load_heatmap(stats, self.rpc_id_dict, rpc_list, 'clients')
        except Exception as e:
            client_heatmap = pn.pane.Markdown(f"{str(e)}", styles=highlight_style)
        try:
            server_heatmap = create_rpc_load_heatmap(stats, self.rpc_id_dict, rpc_list, 'servers')
        except Exception as e:
            server_heatmap = pn.pane.Markdown(f"{str(e)}", styles=highlight_style)
        return pn.Column(
            pn.Column(
                pn.pane.Markdown("### Who's Sending the Most Requests?", styles=sub_section_style),
                pn.pane.Markdown(
                    "This heatmap shows which client processes are making the most RPC calls.",
                    styles=description_style
                ),
                client_heatmap,
                pn.pane.Markdown(
                    get_heatmap_description(view_type='clients'),
                    styles=description_style
                ),
            ),
            pn.Column(
                pn.pane.Markdown("### Who's Handling the Most Work?", styles=sub_section_style),
                pn.pane.Markdown(
                    "This heatmap shows which server processes are handling the most RPC requests. Look for potential overloaded servers.",
                    styles=description_style
                ),
                server_heatmap,
                pn.pane.Markdown(
                    get_heatmap_description(view_type='servers'),
                    styles=description_style
                ),
            )
        )

    def _create_diagnostics_panel(self, stats):
        # Analyze statistics for potential issues
        alerts = self._analyze_general_performance_issues(stats)
    
        return pn.Column(   
            pn.pane.Markdown("## 🔍 Diagnostics Panel", styles=title_style),
            pn.pane.Markdown("### Performance Analysis & Recommendations", styles=sub_section_style),
            *(self._create_alert_panel_components(alerts)),
            styles=border_style
        )
    
    """
    Diagnostic Helper Functions
    Returns:
        List[Dict]: A list of alert dictionaries in the format:
            [
                {
                    'severity': 'low',           # Alert level: 'low', 'medium', or 'high'
                    'title': 'Alert Title',      # Short title for the alert
                    'message': 'Detailed message explaining the alert.'
                },
                ...
            ]
    """
    def _analyze_origin_performance_issues(self, stats, src, dest, src_files):
        alerts = []
            
        if not src_files:
            return alerts
        
        # Get relevant dataframe
        df = get_source_df_given_callpath(stats, src, dest)
        df = df[df.index.get_level_values('address').isin(src_files)]

        # Get relevant metrics from the dataframe
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

        # Analyze metrics and scan for alerts:
        """ Detect serialization bottlenecks:
            1. set_input duration exceeds threshold of iforward duration (e.g., >30%)
            2. Combined set_input + get_output duration dominates total runtime (e.g., >50%)
        """
        set_input_ratio = set_input_duration / iforward_duration
        if set_input_ratio > 0.3:
            alerts.append({
                'severity': 'high',
                'title': 'Serialization Bottleneck',
                'message': f'Set input takes {set_input_ratio:.1%} of forward time. Consider using bulk transfers or optimizing data structure.'
            })              

        serialization_overhead = (set_input_duration + get_output_duration) / total_duration
        if serialization_overhead > 0.5:  # More than 50% of total time
            alerts.append({
                'severity': 'high',
                'title': 'High Serialization Overhead',
                'message': f'Serialization consumes {serialization_overhead:.1%} of total time. Consider data format optimization.'
            })

        """ Detect for blocking calls 
            1. iforward_wait.duration dominates total runtime (e.g., >50%)
            2. spent more time idling than working
        """
        # Long wait times detection
        wait_ratio = wait_duration / total_duration
        if wait_ratio > 0.5:  # More than 50% of time spent waiting
            alerts.append({
                'severity': 'medium',
                'title': 'Blocking Call Detected',
                'message': f'RPC spends {wait_ratio:.1%} of the total time waiting. You were blocked in the iforward_wait method for too long. Consider async patterns or parallel processing.'
            })

        busy_duration = (wait_start - iforward_duration - iforward_start)
        if wait_duration > busy_duration:
            alerts.append({
                'severity': 'low',
                'title': 'Blocking Call Detected',
                'message': f'RPC blocked longer than active work: {format_time(wait_duration)} idle vs {format_time(busy_duration)} work. There is an opportunity to run computations to improve throughput.'
            })

        return alerts

    def _analyze_target_performance_issues(self, stats, src, dest, dest_files):
        alerts = []
        
        if not dest_files:
            return alerts
        
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
        # Analyze metrics and scan for alerts:
        """ Detect serialization bottlenecks:
            1. set_output duration exceeds threshold of irespond duration (e.g., >30%)
            2. Combined get_input + set_output duration dominates total runtime (e.g., >50%)
        """
        set_input_ratio = set_output_duration / irespond_duration
        if set_input_ratio > 0.3:
            alerts.append({
                'severity': 'high',
                'title': 'Serialization Bottleneck',
                'message': f'Set output takes {set_input_ratio:.1%} of forward time. Consider using bulk transfers or optimizing data structure.'
            })              

        serialization_overhead = (get_input_duration + set_output_duration) / total_duration
        if serialization_overhead > 0.5:  # More than 50% of total time
            alerts.append({
                'severity': 'high',
                'title': 'High Serialization Overhead',
                'message': f'Serialization consumes {serialization_overhead:.1%} of total time. Consider data format optimization.'
            })

        """ Detect for blocking calls 
            1. iforward_wait.duration dominates total runtime (e.g., >50%)
            2. spent more time idling than working
        """
        # Long wait times detection
        wait_ratio = wait_duration / total_duration
        if wait_ratio > 0.5:  # More than 50% of time spent waiting
            alerts.append({
                'severity': 'medium',
                'title': 'Blocking Call Detected',
                'message': f'RPC spends {wait_ratio:.1%} of the total time waiting. You were blocked in the iforward_wait method for too long. Consider async patterns or parallel processing.'
            })

        busy_duration = (wait_start - irespond_duration - irespond_start)
        if wait_duration > busy_duration:
            alerts.append({
                'severity': 'low',
                'title': 'Blocking Call Detected',
                'message': f'RPC blocked longer than active work: {format_time(wait_duration)} idle vs {format_time(busy_duration)} work. There is an opportunity to run computations to improve throughput.'
            })

        """ Detect for scheduling issues 
            1. Long scheduling delay before ULT starts
        """
        # Check for delays in handler execution
        if ult_start > handler_duration * 5:  # Significant delay before ULT starts
            alerts.append({
                'severity': 'medium',
                'title': 'Handler Scheduling Delay',
                'message': f'ULT starts {format_time(ult_start)} after handler. Consider using more threads or more servers.'
            })

        return alerts

    def _analyze_general_performance_issues(self, stats):
        alerts = []
        
        # TODO: Analyze stats and check for general performance issues?

        return alerts
    
    def _create_alert_panel_components(self, alerts):
        alert_components = []
        for alert in alerts:
            """
            https://panel.holoviz.org/reference/panes/Alert.html
            For details on other options for customizing the component see the layout and styling how-to guides.
            object (str): The contextual feedback message.
            alert_type (str): The type of Alert and one of primary, secondary, success, danger, warning, info, light, dark.
            """
            if alert['severity'] == 'high':
                alert_component = pn.pane.Alert(f"🚨 **{alert['title']}**\n{alert['message']}", alert_type='danger')
            elif alert['severity'] == 'medium':
                alert_component = pn.pane.Alert(f"⚠️ **{alert['title']}**\n{alert['message']}", alert_type='warning')
            else:
                alert_component = pn.pane.Alert(f"ℹ️ **{alert['title']}**\n{alert['message']}", alert_type='info')
            alert_components.append(alert_component)
        
        # If no issues found, show a success message
        if not alerts:
            alert_components.append(pn.pane.Alert("**No Performance Issues Detected**\nYour RPC performance looks good! All metrics are within normal ranges.", alert_type='success'))
        return alert_components
    