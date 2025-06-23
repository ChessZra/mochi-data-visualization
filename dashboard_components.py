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
    'margin-bottom': '20px'
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
aggregation_mapping = {
    'Total/Sum': 'sum',
    'Average/Mean': 'avg',
    'Maximum': 'max',
    'Minimum': 'min',
    'Count': 'num',
    'Variance': 'var'
}

class MochiDashboard():
    def __init__(self, stats):
        self.rpc_name = {65535: 'None'}
        self.rpc_id = {'None': 65535}

        for df in [stats.origin_rpc_df, stats.target_rpc_df]:
            for index in df.index:
                self.rpc_name[index[3]], self.rpc_id[index[2]] = index[2], index[3]
    
        # Create components
        main_visualization = self._create_main_visualization(stats)
        summary_statistics = self._create_summary_statistics(stats)
        distribution_view = self._create_distribution_view(stats)
        diagnostics_panel = self._create_diagnostics_panel(stats)

        main_page = pn.Column(
            main_visualization, 
            summary_statistics, 
            distribution_view,
            diagnostics_panel
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
        
        template = pn.template.MaterialTemplate(
            title="Mochi Performance Dashboard", 
            header_background="#336699", 
            main=get_page
        )
        template.show()

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
        origin_title = pn.pane.Markdown('', styles=title_style)
        target_title = pn.pane.Markdown('', styles=title_style)

        self.src_files, self.dest_files = [], []

        # Define widget callbacks and functionality
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

            graph_wrapper.append(create_per_rpc_bar_plot(stats, callpath_src, callpath_dest, self.src_files, self.dest_files))
            svg_origin_wrapper.append(create_per_rpc_svg_origin(stats, callpath_src, callpath_dest, self.src_files))
            svg_target_wrapper.append(create_per_rpc_svg_target(stats, callpath_src, callpath_dest, self.dest_files))

            origin_title.object = f"### Sender Performance Metrics"
            target_title.object = f"### Receiver Performance Metrics"

        # Dynamic plots
        graph_wrapper = pn.Column(create_per_rpc_bar_plot(stats, -1, -1, [], []))
        svg_origin_wrapper = pn.Column(create_per_rpc_svg_origin(stats, -1, -1, []))
        svg_target_wrapper = pn.Column(create_per_rpc_svg_target(stats, -1, -1, []))
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
            pn.Row(
                pn.Column(
                    pn.pane.Markdown(f"### RPC from the sender's point of view ({src})", styles=title_style),
                    pn.pane.SVG("./img/rpc-origin.svg", width=300, height=400)
                ),
                pn.Column(origin_title, svg_origin_wrapper), 
            ),
            pn.Row(                 
                pn.Column(
                    pn.pane.Markdown(f"### RPC from the receiver's point of view ({dest})", styles=sub_section_style),
                    pn.pane.SVG("./img/rpc-target.svg", width=300, height=400)
                ),
                pn.Column(target_title, svg_target_wrapper),
            ),
            styles=border_style
        )

    def _create_main_visualization(self, stats):
        metric_dropdown = pn.widgets.Select(name='Metric', options=['RPC Execution Time', 'Client Call Time', 'Bulk Transfer Time', 'RDMA Data Transfer Size'], value='RPC Execution Time')
        aggregation_dropdown = pn.widgets.Select(name='Aggregation', options=['Total/Sum', 'Average/Mean', 'Maximum', 'Minimum', 'Count', 'Variance'], value='Average/Mean')

        @pn.depends(metric_dropdown, aggregation_dropdown)
        def get_visualization(metric_choice, aggregation_choice):
            agg_method = aggregation_mapping[aggregation_choice]
            bars = create_main_plot(stats, metric_choice, agg_method, self.rpc_name)
        
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
                pn.pane.Markdown("## 📊 Main Visualization", styles=title_style),
                pn.pane.Markdown("### Performance Metrics Analysis", styles=sub_section_style),
                pn.Row(metric_dropdown, aggregation_dropdown),
                get_visualization,
                pn.pane.Markdown("💡 **Tip:** Click on any bar to view detailed per-RPC statistics for that specific RPC call", styles={'font-style': 'italic', 'font-size': '14px'}),
                styles=border_style
            ),
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
        )
        
        server_heatmap_section = pn.Column(
            pn.pane.Markdown("### RPC Load: Servers (calls handled)", styles={'color': '#34495e'}), 
            create_rpc_load_heatmap(stats, view_type='servers'),
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
        alerts = self._analyze_performance_issues(stats)
        
        # Create alert components
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
        
        return pn.Column(   
            pn.pane.Markdown("## 🔍 Diagnostics Panel", styles=title_style),
            pn.pane.Markdown("### Performance Analysis & Recommendations", styles=sub_section_style),
            *alert_components,
            styles=border_style
        )
    
    def _analyze_performance_issues(self, stats):
        alerts = []
        
        self._check_serialization_issues(stats, alerts)
        self._check_blocking_calls(stats, alerts)
        self._check_scheduling_issues(stats, alerts)

        return alerts
    
    def _check_serialization_issues(self, stats, alerts):
        
        # TODO: Analyze stats and check for performance issues
        
        alerts.append({
            'severity': 'high',
            'title': 'Serialization Performance Issue',
            'message': f'The RPC is spending a lot of time serializing. Consider using bulk transfers for large data to improve performance.'
        })
        return alerts
    
    def _check_blocking_calls(self, stats, alerts):

        # TODO: Analyze stats and check for performance issues

        alerts.append({
            'severity': 'low',
            'title': 'Blocking Call Performance Issue',
            'message': f'The RPC is spending a lot of time waiting for a callback. There is an opportunity for concurrency while waiting for forward_cb.'
        })
        return alerts
    
    def _check_scheduling_issues(self, stats, alerts):

        # TODO: Analyze stats and check for performance issues

        alerts.append({
            'severity': 'medium',
            'title': 'Scheduling Performance Issue',
            'message': f'The RPC is spending a lot of time waiting for the handler. Potential scheduling issue, consider using more threads or more servers.'
        })

        return alerts
    