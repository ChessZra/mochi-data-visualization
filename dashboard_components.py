import panel as pn

import holoviews as hv
import hvplot.pandas

from holoviews.streams import Tap
from plotting_functions import *
from static_functions import *

title_style = {
    'font-size': '24px',
    'font-weight': 'bold',
    'color': '#2c3e50',
}
border_style = {
    'background': '#ffffff',
    'padding': '20px',
    'border-radius': '10px',
    'box-shadow': '0 2px 4px rgba(0,0,0,0.1)'
}
sub_section_style = {
    'font-size': '18px',
    'color': '#34495e',
}
description_style = {
    "font-size": "14px",
    "color": "#555",
    "line-height": "1.5",
    "white-space": "pre-wrap", 
    "overflow-wrap": "break-word",
    "max-width": "800px", 
}

class MochiDashboard():
    def __init__(self, stats):
        self.rpc_name = {65535: 'None'}
        self.rpc_id = {'None': 65535}

        for df in [stats.origin_rpc_df, stats.target_rpc_df]:
            for index in df.index:
                self.rpc_name[index[3]], self.rpc_id[index[2]] = index[2], index[3]

        main_page = pn.Column(
            pn.pane.Markdown("## 📊 Main Visualization", styles=title_style),
            self._create_section_one(stats),
            self._create_section_two(stats),
            self._create_section_three(stats),
            self._create_distribution_view(stats),
            self._create_summary_statistics(stats),
            self._create_diagnostics_panel(stats), 
        )
        
        # Main functionality to trigger different pages (from main page to rpc-per page)
        # self.trigger.value = "context" (to trigger the function with 'context')
        self.trigger = pn.widgets.TextInput(name='Page Toggle', value='', visible=False)
        @pn.depends(self.trigger)
        def get_page(context):
            if not context or context == 'back_to_main_page':
                return main_page
            else:
                return self._create_per_rpc_statistics(context, stats)
        
        template = pn.template.MaterialTemplate(title="Mochi Performance Dashboard", header_background="#336699", main=get_page)
        template.show()

    def _create_section_one(self, stats):
        return pn.Column(pn.pane.Markdown("### Section 1: Process Overview", styles=sub_section_style), create_graph_1(stats), pn.pane.Markdown(get_graph_1_description(), styles=description_style), create_graph_2(stats), pn.pane.Markdown(get_graph_2_description(), styles=description_style), styles=border_style)

    def _create_section_two(self, stats):
        process_dropdown = pn.widgets.Select(name='Process', options=get_all_addresses(stats))

        @pn.depends(process_dropdown)
        def get_graph_3(process_choice):
            try:
                ret = create_graph_3(stats, process_choice, self.rpc_name)
                return ret
            except:
                return pn.pane.Markdown("This process doesn't have any values.")
            
        @pn.depends(process_dropdown)
        def get_graph_4(process_choice):
            try:
                ret = create_graph_4(stats, process_choice, self.rpc_name)
                return ret
            except:
                return pn.pane.Markdown("This process doesn't have any values.")
        
        return pn.Column(pn.pane.Markdown("### Section 2: Process Deep Dive", styles=sub_section_style), process_dropdown, get_graph_3, pn.pane.Markdown(get_graph_3_description(), styles=description_style), get_graph_4, pn.pane.Markdown(get_graph_4_description(), styles=description_style), styles=border_style)

    def _create_section_three(self, stats):
        metric_dropdown = pn.widgets.Select(name='Metric', options=['Server Execution Time', 'Client Call Time', 'Bulk Transfer Time', 'RDMA Data Transfer Size'], value='Server Execution Time')

        @pn.depends(metric_dropdown)
        def get_visualization(metric_choice):

            bars = create_graph_5(stats, metric_choice, self.rpc_name)
        
            #  Bar click callback
            tap = Tap(source=bars)
            @pn.depends(tap.param.x)
            def on_bar_click(x):
                if x is not None:
                    self.trigger.value = x # Trigger the next page
                return ''
            return pn.Column(bars, on_bar_click)
        
        return pn.Row(
            pn.Column(
                pn.pane.Markdown("### Section 3: RPC Analysis", styles=sub_section_style),
                pn.Row(metric_dropdown),
                get_visualization,
                pn.pane.Markdown(get_graph_5_description(), styles=description_style),
                pn.pane.Markdown("💡 **Tip:** Click on any bar to view detailed per-RPC statistics for that specific RPC call", styles={'font-style': 'italic', 'font-size': '14px'}),
                styles=border_style
            ),
        )

    def _create_per_rpc_statistics(self, context, stats):
        # Parse the context (which is wrapped with \n)
        if '➔' in context:
            src, dest = context[:context.index('➔')].replace('\n', ''), context[context.index('➔') + 1:].replace('\n', '').replace(' ', '')
        else:
            src, dest = 'None', context.replace('\n', '')

        # Remember: 65535:65535:<rpc_id>:<provider_id>
        callpath_src = self.rpc_id[src]
        callpath_dest = self.rpc_id[dest]

        # Define variables/widgets
        back_to_main_page_button = pn.widgets.Button(name='Back to main page')
        apply_button = pn.widgets.Button(name='Apply')    
        origin_select = pn.widgets.MultiChoice(name='Source', options=get_source_addresses_given_callpath(stats, callpath_src, callpath_dest))
        target_select = pn.widgets.MultiChoice(name='Destination', options=get_dest_addresses_given_callpath(stats, callpath_src, callpath_dest))

        self.src_files, self.dest_files = [], []

        # Define widget callbacks 
        def origin_on_change(event):
            self.src_files = event.new
        
        def target_on_change(event):
            self.dest_files = event.new
        
        def on_back_button_click(event):
            self.trigger.value = 'back_to_main_page'
                
        def on_apply_button_click(event):
            graph_wrapper.clear()
            svg_origin_wrapper.clear()
            svg_target_wrapper.clear()
            origin_diagnostics_panel.clear()
            target_diagnostics_panel.clear()

            # Display bar graph
            graph_wrapper.append(create_per_rpc_bar_plot(stats, callpath_src, callpath_dest, self.src_files, self.dest_files))

            # Display svgs
            svg_origin_wrapper.append(create_per_rpc_svg_origin(stats, callpath_src, callpath_dest, self.src_files))
            svg_target_wrapper.append(create_per_rpc_svg_target(stats, callpath_src, callpath_dest, self.dest_files))

            # Display alerts
            origin_rpc_alerts = self._analyze_origin_performance_issues(stats, callpath_src, callpath_dest, self.src_files)
            target_rpc_alerts = self._analyze_target_performance_issues(stats, callpath_src, callpath_dest, self.dest_files)
            origin_diagnostics_panel.append(pn.Column(*(self._create_alert_panel_components(origin_rpc_alerts))))
            target_diagnostics_panel.append(pn.Column(*(self._create_alert_panel_components(target_rpc_alerts))))

        # Create panel components
        graph_wrapper = pn.Column()
        svg_origin_wrapper = pn.Column()
        svg_target_wrapper = pn.Column()
        origin_diagnostics_panel = pn.Column()
        target_diagnostics_panel = pn.Column()
        
        # Define widget functionality
        target_select.param.watch(target_on_change, 'value')
        origin_select.param.watch(origin_on_change, 'value')
        back_to_main_page_button.on_click(on_back_button_click)
        apply_button.on_click(on_apply_button_click)
        
        # Return layout
        return pn.Column(
            back_to_main_page_button,
            pn.pane.Markdown(f"## Detail View: {dest}" if src == 'None' else f'## Detail View: {src} ➔ {dest}', styles=title_style),           
            pn.Row(origin_select, target_select, apply_button), 
            graph_wrapper,
            pn.Column(
                pn.pane.Markdown(f"### RPC from the sender's point of view ({src}):", styles=sub_section_style),
                pn.Row(
                    pn.pane.SVG("./img/rpc-origin.svg", width=300, height=400),
                    pn.Column(svg_origin_wrapper, styles=sub_section_style), 
                ),
                origin_diagnostics_panel
            ),
            pn.Column(                             
                pn.pane.Markdown(f"### RPC from the receiver's point of view ({dest}):", styles=sub_section_style),
                pn.Row(
                    pn.pane.SVG("./img/rpc-target.svg", width=300, height=400),
                    pn.Column(svg_target_wrapper, styles=sub_section_style),
                ),
                target_diagnostics_panel
            ),
            styles=border_style
        )
        
    def _create_summary_statistics(self, stats):
        return pn.Column(
            pn.pane.Markdown("## 📋 Summary Statistics", styles=title_style),
            pn.Row(
                pn.Column(
                    pn.pane.Markdown("### Total RPCs", styles={'color': '#34495e'}),
                    pn.widgets.TextInput(
                        name='Total RPCs',
                        value=str(get_number_of_rpc_calls(stats)),
                        disabled=False,
                        styles={'background': '#f8f9fa', 'border-radius': '5px'},
                        width=235
                    )
                ),
                pn.Column(
                    pn.pane.Markdown("### Avg Execution Time", styles={'color': '#34495e'}),
                    pn.widgets.TextInput(
                        name='Avg Execution Time',
                        value=format_time(get_average_execution_time(stats)),
                        disabled=False,
                        styles={'background': '#f8f9fa', 'border-radius': '5px'},
                        width=235
                    )
                ),
                pn.Column(
                    pn.pane.Markdown("### Max Execution Time", styles={'color': '#34495e'}),
                    pn.widgets.TextInput(
                        name='Max Execution Time',
                        value=format_time(get_max_execution_time(stats)),
                        disabled=False,
                        styles={'background': '#f8f9fa', 'border-radius': '5px'},
                        width=235
                    )
                ),
                pn.Column(
                    pn.pane.Markdown("### Total Data Transferred", styles={'color': '#34495e'}),
                    pn.widgets.TextInput(
                        name='Total Data Transferred',
                        value=format_data_size(get_total_data_transferred(stats)),
                        disabled=False,
                        styles={'background': '#f8f9fa', 'border-radius': '5px'},
                        width=235   
                    )
                )
            ),
            styles=border_style
        ) 
   
    def _create_distribution_view(self, stats):
        client_heatmap_section = pn.Column(
            pn.pane.Markdown("### RPC Load: Clients (calls sent)", styles={'color': '#34495e'}), 
            create_rpc_load_heatmap(stats, view_type='clients'),
            pn.pane.Markdown(get_heatmap_description(view_type='clients'), styles=description_style),
        )
        
        server_heatmap_section = pn.Column(
            pn.pane.Markdown("### RPC Load: Servers (calls handled)", styles={'color': '#34495e'}), 
            create_rpc_load_heatmap(stats, view_type='servers'),
            pn.pane.Markdown(get_heatmap_description(view_type='servers'), styles=description_style),
        )
        
        graph_section = pn.Column(
            pn.pane.Markdown("### RPC Communication Graph (Processes as nodes, RPCs as edges)", styles={'color': '#34495e'}),
            pn.pane.Markdown("**Legend:** 🔵 Client  🟢 Server  🟠 Both"),    
            create_communication_graph(stats)
        )
        
        return pn.Column(  
            pn.pane.Markdown("## 📈 Distribution View", styles=title_style),
            pn.pane.Markdown("### RPC Load Distribution", styles=sub_section_style),
            client_heatmap_section,
            server_heatmap_section,
            graph_section,
            styles=border_style
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
            alert_components.append(pn.pane.Alert("✅ **No Performance Issues Detected**\nYour RPC performance looks good! All metrics are within normal ranges.", alert_type='success'))
        return alert_components