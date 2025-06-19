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
sub_title = {
    'font-size': '18px',
    'color': '#34495e',
    'margin-bottom': '15px'
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
        # Parse the context
        if '➔' in context:
            """
            context is in the format:
            origin_name\n➔ target_name
            or:
            target_name (without origin)
            """
            src = context[:context.index('➔') - 1] 
            dest = context[context.index('➔') + 2:]
        else:
            src = 'None'
            dest = context.replace('\n', '')

        # Remember: 65535:65535:<rpc_id>:<provider_id>
        callpath_src = self.rpc_id[src]
        callpath_dest = self.rpc_id[dest]

        # Define variables/widgets
        back_to_main_page_button = pn.widgets.Button(name='Back to main page')
        apply_button = pn.widgets.Button(name='Apply')    
        origin_select = pn.widgets.MultiChoice(name='Source', options=get_source_addresses_given_callpath(stats, callpath_src, callpath_dest))
        target_select = pn.widgets.MultiChoice(name='Destination', options=get_dest_addresses_given_callpath(stats, callpath_src, callpath_dest))
        origin_title = pn.pane.Markdown('', styles=sub_title)
        target_title = pn.pane.Markdown('', styles=sub_title)

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
                pn.pane.Markdown(f"## Detail View: {dest}" if src == 'None' else f'## Detail View: {src} ➔ {dest}', styles=sub_title),           
                pn.Row(origin_select, target_select, apply_button), 
                graph_wrapper,
                pn.Row(
                    pn.Column(
                        pn.pane.Markdown("### RPC from the sender's point of view (origin)", styles=sub_title),
                        pn.pane.SVG("./img/rpc-origin.svg", width=300, height=400)
                    ),
                    pn.Column(origin_title, svg_origin_wrapper), 
                ),
                pn.Row(                 
                    pn.Column(
                        pn.pane.Markdown("### RPC from the receiver's point of view (target)", styles=sub_title),
                        pn.pane.SVG("./img/rpc-target.svg", width=300, height=400)
                    ),
                    pn.Column(target_title, svg_target_wrapper),
                ),
                styles=border_style)

    def _create_main_visualization(self, stats):

        metric_dropdown = pn.widgets.Select(name='Metric', options=['RPC Execution Time', 'Client Call Time', 'Bulk Transfer Time', 'RDMA Data Transfer Size'], value='RPC Execution Time')
        aggregation_dropdown = pn.widgets.Select(name='Aggregation', options=['Total/Sum', 'Average/Mean', 'Maximum', 'Minimum', 'Count', 'Variance'], value='Average/Mean')

        @pn.depends(metric_dropdown, aggregation_dropdown)
        def get_visualization(metric_choice, aggregation_choice):
            agg_method = aggregation_mapping[aggregation_choice]
            bars = create_main_plot(stats, metric_choice, agg_method, self.rpc_name)
        
            # === Bar click functionality ===
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
                pn.pane.Markdown("### Performance Metrics Analysis", styles=sub_title),
                pn.Row(metric_dropdown, aggregation_dropdown),
                get_visualization,
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
                        width=185
                    )
                ),
                pn.Column(
                    pn.pane.Markdown("### Avg Execution Time", styles={'color': '#34495e'}),
                    pn.widgets.TextInput(
                        name='Avg Execution Time',
                        value=format_time(get_average_execution_time(stats)),
                        disabled=False,
                        styles={'background': '#f8f9fa', 'border-radius': '5px'},
                        width=185
                    )
                ),
                pn.Column(
                    pn.pane.Markdown("### Max Execution Time", styles={'color': '#34495e'}),
                    pn.widgets.TextInput(
                        name='Max Execution Time',
                        value=format_time(get_max_execution_time(stats)),
                        disabled=False,
                        styles={'background': '#f8f9fa', 'border-radius': '5px'},
                        width=185
                    )
                ),
                pn.Column(
                    pn.pane.Markdown("### Total Data Transferred", styles={'color': '#34495e'}),
                    pn.widgets.TextInput(
                        name='Total Data Transferred',
                        value=format_data_size(get_total_data_transferred(stats)),
                        disabled=False,
                        styles={'background': '#f8f9fa', 'border-radius': '5px'},
                        width=185   
                    )
                )
            ),
            styles=border_style
        ) 
   
    def _create_distribution_view(self, stats):
        client_heatmap = create_rpc_load_heatmap(stats, view_type='clients')
        server_heatmap = create_rpc_load_heatmap(stats, view_type='servers')
        # Create separate layouts to avoid plot interference
        heatmap_section = pn.Column(
            pn.pane.Markdown("### RPC Load Distribution", styles=sub_title),
            pn.Row(
                pn.Column(
                    pn.pane.Markdown("### RPC Load: Clients (calls sent)", styles=sub_title), 
                    client_heatmap
                ),
                pn.Column(
                    pn.pane.Markdown("### RPC Load: Servers (calls handled)", styles=sub_title), 
                    server_heatmap
                )
            )
        )
        
        graph_section = pn.Column(
            pn.pane.Markdown("### RPC Communication Graph (Processes as nodes, RPCs as edges)", styles=sub_title),
            pn.pane.Markdown("**Legend:** 🔵 Client  🟢 Server  🟠 Both"),    
            create_node_graph(stats)
        )
        
        return pn.Column(  
            pn.pane.Markdown("## 📈 Distribution View", styles=title_style),
            heatmap_section,
            graph_section,
            styles=border_style
        )

    def _create_diagnostics_panel(self, stats):
        return pn.Column(   
            pn.pane.Markdown("## 🔍 Diagnostics Panel", styles=title_style),
            pn.Row(
                pn.widgets.TextInput(name='⚠️Potential Issue:', width=390),
                pn.widgets.TextInput(name='⚠️Potential Issue:', width=390),
            ),
            styles=border_style
        )