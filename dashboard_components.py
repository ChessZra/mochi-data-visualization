"""
Mochi Data Visualization - Dashboard Components

This module provides the main dashboard UI and interactive components for visualizing RPC (Remote Procedure Call)
performance data in distributed systems. It leverages Panel and HoloViews to create a user-friendly, interactive
web dashboard for exploring Mochi performance statistics.

DEVELOPMENT WORKFLOW:
=====================
- All graph visualizations are first prototyped and tested in a Jupyter notebook for rapid iteration and debugging.
- Once a graph is working as intended, its logic is moved into a function in `plotting_functions.py`.
- The dashboard GUI and interactive components are developed and tested in the `MochiDashboard` class (in this file).
- The final step is to connect the plotting functions from `plotting_functions.py` to the dashboard components.

This workflow allows for fast prototyping, easy debugging, and modular code organization.

GENERAL METHODOLOGY:
====================

1. DASHBOARD STRUCTURE:
    - The main class is `MochiDashboard`, which builds the dashboard layout and navigation.
    - The dashboard is organized into sections: Overview, Process Analysis, Detailed Analysis, and Advanced Analysis.
    - Each section is created by a dedicated method (e.g., `_create_overview_section`).

2. DATA ACCESS & VISUALIZATION:
    - The dashboard expects a `stats` object containing DataFrames (origin_rpc_df, target_rpc_df, bulk_transfer_df).
    - Visualization functions (e.g., `create_graph_1`, `create_graph_2`, etc.) are imported from `plotting_functions.py`.
    - Interactive widgets (dropdowns, buttons) allow users to select processes, metrics, and drill down into details. 

3. STYLING & USER EXPERIENCE:
    - Consistent style dictionaries (e.g., TITLE_STYLE, SECTION_STYLE) are used for a better look.

4. INTERACTIVITY:
    - Uses Panel's dependency system (`@pn.depends`) for dynamic updates based on user input.
    - Tap and selection events allow users to click on bars or tables to explore deeper statistics.
    - Navigation between main and per-RPC views is seamless and stateful.
    - The <get_page> function is in charge of switching the views between the main page and the PER-RPC page.
    - The <get_page> function is called everytime <self.trigger> is assigned a new value.

EXTENDING THE DASHBOARD:
========================

To add a new section or visualization:
    1. Create a new method in `MochiDashboard` for the section or feature.
    2. Use or create new plotting functions in `plotting_functions.py` as needed.
    3. Add new widgets or interactivity using Panel components.
    4. Update the main layout in the constructor to include the new section.

COMMON DATA STRUCTURES:
=======================
    - stats.origin_rpc_df: Multi-index DataFrame with client-side RPC data
    - stats.target_rpc_df: Multi-index DataFrame with server-side RPC data
    - stats.bulk_transfer_df: DataFrame with RDMA transfer data
    - rpc_name_dict: Dict mapping RPC IDs to names
    - rpc_id_dict: Dict mapping RPC names to IDs

COMMON COMPONENTS:
==================
    - MochiDashboard: Main dashboard class
    - _create_header, _create_overview_section, etc.: Section builder methods
    - Panel widgets: Dropdowns, buttons, tables for interactivity
    - Styling dicts: For consistent look and feel

"""
import panel as pn

from holoviews.streams import Tap
from plotting_functions import *

TITLE_STYLE = {
    'font-size': '28px',
    'font-weight': 'bold',
    'color': '#2c3e50',
    'margin-bottom': '20px',
    'text-align': 'center'
}

SECTION_STYLE = {
    'font-size': '22px',
    'font-weight': '600',
    'color': '#34495e',
    'margin-bottom': '15px',
    'padding-bottom': '10px',
    'border-bottom': '2px solid #ecf0f1'
}

SUB_SECTION_STYLE = {
    'font-size': '18px',
    'font-weight': '500',
    'color': '#34495e',
    "white-space": "pre-wrap", 
    "overflow-wrap": "break-word",
    "max-width": "1000px", 
    'margin-bottom': '10px'
}

DESCRIPTION_STYLE = {
    "font-size": "15px",
    "color": "#555",
    "line-height": "1.6",
    "white-space": "pre-wrap", 
    "overflow-wrap": "break-word",
    "max-width": "1000px", 
    'margin-bottom': '15px'
}

BORDER_STYLE = {
    'background': '#ffffff',
    'padding': '25px',
    'margin': '15px 0',
    'border-radius': '12px',
    'box-shadow': '0 4px 6px rgba(0,0,0,0.1)',
    'border-left': '4px solid #3498db'
}

HIGHLIGHT_STYLE = {
    'background': '#f8f9fa',
    'padding': '20px',
    'margin': '15px 0',
    'border-radius': '8px',
    'border-left': '4px solid #e74c3c',
    'font-size': '14px',
    "max-width": "1000px",
    'color': '#2c3e50'
}

TIP_STYLE = {
    'background': '#e8f4fd',
    'padding': '15px',
    'margin': '10px 0',
    'border-radius': '8px',
    'border-left': '4px solid #3498db',
    'font-size': '14px',
    "max-width": "1000px",
    'color': '#2c3e50'
}

SIDE_NOTE_STYLE = {
    'font-style': 'italic', 
    'color': '#7f8c8d', 
    'font-size': '13px',
    'background': '#f8f9fa',
    'padding': '10px',
    'border-radius': '6px',
    'margin': '10px 0'
}
MAIN_PAGE_WIDTH = 1100
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
            self._create_advanced_analysis_section(stats),
            styles={'max-width': '1200px', 'margin': '0 auto'}
        )
        
        # Navigation system
        self.trigger = pn.widgets.TextInput(name='Page Toggle', value='', visible=False)
        @pn.depends(self.trigger)
        def get_page(context):
            if not context or context == 'back_to_main_page':
                print("Attempting to display main_page")
                return main_page
            elif context == 'per_rpc_analysis':
                print("Attempting to display PER-RPC page")
                return self._create_per_rpc_view(stats)
            else:
                print("Attempting to display PER-RPC page")
                return self._create_per_rpc_view(stats)
        
        template = pn.template.MaterialTemplate(
            title="Mochi Performance Dashboard", 
            header_background="#2c3e50",
            main=get_page
        )
        template.show()
    
    """ General Overview Section (Main Page)"""
    def _create_header(self):
        return pn.Column(
            pn.pane.Markdown(
                "# Mochi Performance Dashboard",
                styles=TITLE_STYLE
            ),
            pn.pane.Markdown(
                "**Welcome!** This dashboard helps you understand how your distributed system is performing. "
                "Start with the overview to get the big picture, then dive deeper into specific processes and RPC calls.",
                styles=DESCRIPTION_STYLE
            ),
            pn.pane.Markdown(
                "**Quick Start:** Scroll down to see system-wide performance, then select a process to analyze its behavior, "
                "and finally click on any RPC bar to explore detailed statistics.",
                styles=TIP_STYLE
            ),
            styles=BORDER_STYLE,
            width=MAIN_PAGE_WIDTH
        )

    def _create_overview_section(self, stats):
        """Create high-level system overview with user-friendly explanations"""
        return pn.Column(
            pn.pane.Markdown("## System Overview", styles=SECTION_STYLE),
            pn.pane.Markdown(
                "Let's start with the big picture. These charts show you which processes are doing the most work "
                "and how your system is communicating overall.",
                styles=DESCRIPTION_STYLE
            ),
            pn.Column(
                create_graph_1(stats),
                pn.pane.Markdown(
                    get_graph_1_description(),
                    styles=DESCRIPTION_STYLE
                ),
            ),
            pn.Column(
                create_graph_2(stats),
                pn.pane.Markdown(
                    get_graph_2_description(),
                    styles=DESCRIPTION_STYLE
                ),
            ),
            pn.pane.Markdown(
                "**Look for:** Processes with unusually high bars - they might be bottlenecks or need optimization.",
                styles=SIDE_NOTE_STYLE
            ),
            styles=BORDER_STYLE,
            width=MAIN_PAGE_WIDTH
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
                    styles=DESCRIPTION_STYLE
                )
            
            try:
                client_view = create_graph_3(stats, process_choice, self.rpc_name_dict)
            except:
                client_view = pn.pane.Markdown(
                    f"**No data available for {process_choice}** - This process may not have made any RPC calls.",
                    styles=HIGHLIGHT_STYLE
                )
            
            try:
                server_view = create_graph_4(stats, process_choice, self.rpc_name_dict)
            except:
                server_view = pn.pane.Markdown(
                    f"**No data available for {process_choice}** - This process may not have handled any RPC calls.",
                    styles=HIGHLIGHT_STYLE
                )

            return pn.Column(
                pn.Column(
                    pn.pane.Markdown("**As a Client** (RPCs it calls)", styles=SUB_SECTION_STYLE),
                    client_view,
                    pn.pane.Markdown(
                        get_graph_3_description(),
                        styles=DESCRIPTION_STYLE
                    ),
                ),
                pn.Column(
                    pn.pane.Markdown("**As a Server** (RPCs it handles)", styles=SUB_SECTION_STYLE),
                    server_view,
                    pn.pane.Markdown(
                        get_graph_4_description(),
                        styles=DESCRIPTION_STYLE
                    ),
                )
            )
        
        return pn.Column(
            pn.pane.Markdown("## Process Deep Dive", styles=SECTION_STYLE),
            pn.pane.Markdown(
                "Now let's look at individual processes. Select one to see how it behaves as both a client "
                "(making calls) and a server (handling requests).",
                styles=DESCRIPTION_STYLE
            ),
            process_dropdown,
            get_process_analysis,
            styles=BORDER_STYLE,
            width=MAIN_PAGE_WIDTH
        )

    def _create_detailed_analysis_section(self, stats):
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
                    styles=DESCRIPTION_STYLE
                )
            )
        
        return pn.Column(
            pn.pane.Markdown("## RPC Performance Analysis", styles=SECTION_STYLE),
            pn.pane.Markdown(
                "Here you can see which RPC calls are using the most resources in your system. "
                "Pick a metric (like execution time or transfer size) to see which operations are the most demanding.",
                styles=DESCRIPTION_STYLE
            ),
                pn.Row(metric_dropdown),
                get_visualization,
            pn.pane.Markdown(
                "**Pro Tip:** Start with 'Server Execution Time' to find the slowest operations, "
                "then explore 'Client Call Time' to see if delays are in the network or processing.",
                styles=TIP_STYLE
            ),
            styles=BORDER_STYLE,
            width=MAIN_PAGE_WIDTH
        )

    def _create_advanced_analysis_section(self, stats):
        """Create a section with a button to access the PER-RPC analysis page"""
        per_rpc_button = pn.widgets.Button(
            name='Advanced PER-RPC Analysis', 
            button_type='primary',
            width=300,
            height=50
        )
        
        def on_per_rpc_button_click(event):
            self.trigger.value = 'per_rpc_analysis'
        
        per_rpc_button.on_click(on_per_rpc_button_click)
        
        return pn.Column(
            pn.pane.Markdown(
                "Ready to dive deeper? This advanced analysis lets you investigate specific RPC calls in detail.",
                styles=DESCRIPTION_STYLE
            ),
            pn.pane.Markdown(
                "• **Pick specific processes** and see how they communicate\n"
                "• **Choose individual RPC calls** to analyze their performance step-by-step\n"
                "• **Find bottlenecks** by looking at timing breakdowns\n"
                "• **Spot performance issues** by analyzing how consistent the timing is",
                styles=DESCRIPTION_STYLE
            ),
            pn.Row(
                per_rpc_button,
                pn.pane.Markdown(
                    "**When to use this:** When you've found a specific process or RPC call that seems problematic and want to understand why",
                    styles=TIP_STYLE
                )
            ),
            styles=BORDER_STYLE,
            width=MAIN_PAGE_WIDTH
        )

    """ PER-RPC Section """
    def _create_per_rpc_view(self, stats):
        """Create the detailed RPC view with improved user guidance"""
        # Define components
        all_options = get_all_addresses(stats)
        origin_select = pn.widgets.MultiSelect(
            name='Source Processes', 
            options=all_options, 
            size=8, 
            width=300,
            height=150,
        )
        target_select = pn.widgets.MultiSelect(
            name='Destination Processes', 
            options=all_options, 
            size=8, 
            width=300,
            height=150,
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
            print("This may take a while...")
            
            rpc_list = []
            for row_index in self.tab_selection:
                src_address, dst_address = rpc_table_wrapper[0][2].value.iloc[row_index]['Source'], rpc_table_wrapper[0][2].value.iloc[row_index]['Target']
                RPC = rpc_table_wrapper[0][2].value.loc[row_index]['RPC']
                rpc_list.append((src_address, dst_address, RPC))
            
            right_layout.clear()
            # Add loading indicator
            loading_indicator = pn.indicators.LoadingSpinner(value=True, width=50, height=50, color='primary')
            right_layout.append(pn.Column(loading_indicator, pn.pane.Markdown("""Loading detailed analysis... (this may take a while)\n\nFirst run? It can take longer as we build the cache. Subsequent runs will be faster.""", styles=DESCRIPTION_STYLE)))

            # This method will take a while
            detailed_rpc_layout = self._create_detailed_per_rpc_layout(stats, rpc_list)            
        
            right_layout.clear()
            right_layout.append(detailed_rpc_layout)

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
                    pn.pane.Markdown("### Step 2: Select RPCs to Analyze", styles=SUB_SECTION_STYLE),
                    pn.pane.Markdown(
                        f"Found **{len(df)}** RPC communication(s) between your selected processes. Choose which ones you want to analyze in detail: ",
                        styles=DESCRIPTION_STYLE
                    ),
                    table,
                    pn.pane.Markdown(
                        "**Selection Tips:** Click to select individual rows, Ctrl+click for multiple selections, or Shift+click for ranges",
                        styles=TIP_STYLE
                    ),
                    confirm_button,
                    pn.pane.Markdown('*Note: You may notice provider IDs are included in the logs. In our case, RPC grouping ignores provider ID and parent provider ID for simplified analysis*',styles=SIDE_NOTE_STYLE),  
                    styles=BORDER_STYLE,
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
                    pn.pane.Markdown(f"## PER-RPC Statistics Page", styles=SECTION_STYLE),
                    pn.pane.Markdown(
                        "You're now analyzing a specific RPC type. Select the source and destination processes "
                        "you want to examine, then choose which specific RPC calls to analyze in detail.",
                        styles=DESCRIPTION_STYLE
                    ),
                    pn.pane.Markdown("### Step 1: Choose Your Processes", styles=SUB_SECTION_STYLE),
                    pn.Row(
                        pn.pane.Markdown("• **Source**: Processes that initiate RPC calls (clients)\n• **Destination**: Processes that receive and handle RPC calls (servers)", styles=DESCRIPTION_STYLE),
                        apply_button    
                    ),
                    pn.Row(origin_select, target_select),
                    styles=BORDER_STYLE,
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
            chord_graph_client_view = create_chord_graph(stats, self.rpc_id_dict, rpc_list, view_type='clients')
        except Exception as e:
            chord_graph_client_view = pn.pane.Markdown(
                f"{str(e)}",
                styles=HIGHLIGHT_STYLE
            )

        try:
            chord_graph_server_view = create_chord_graph(stats, self.rpc_id_dict, rpc_list, view_type='servers')
        except Exception as e:
            chord_graph_server_view = pn.pane.Markdown(
                f"{str(e)}",
                styles=HIGHLIGHT_STYLE
            )

        try:
            graph_6_view = create_graph_6(stats, self.rpc_id_dict, rpc_list)
        except Exception as e:
            graph_6_view = pn.pane.Markdown(
                f"{str(e)}",
                styles=HIGHLIGHT_STYLE
            )
        try:
            graph_7_view = create_graph_7(stats, self.rpc_id_dict, rpc_list)
        except Exception as e:
            graph_7_view = pn.pane.Markdown(
                f"{str(e)}",
                styles=HIGHLIGHT_STYLE
            )
        try:
            graph_8_view = create_graph_8(stats, self.rpc_id_dict, rpc_list)
        except Exception as e:
            graph_8_view = pn.pane.Markdown(
                f"{str(e)}",
                styles=HIGHLIGHT_STYLE
            )
        try:
            graph_9_view = create_graph_9(stats, self.rpc_id_dict, rpc_list)
        except Exception as e:
            graph_9_view = pn.pane.Markdown(
                f"{str(e)}",
                styles=HIGHLIGHT_STYLE
            )
        try:
            graph_10_view = create_graph_10(stats, self.rpc_id_dict, rpc_list)
        except Exception as e:
            graph_10_view = pn.pane.Markdown(
                f"{str(e)}",
                styles=HIGHLIGHT_STYLE
            )

        try:
            svg_origin = create_per_rpc_svg_origin(stats, self.rpc_id_dict, rpc_list)
        except Exception as e:
            svg_origin = pn.pane.Markdown(
                f"{str(e)}",
                width=450,
                styles=HIGHLIGHT_STYLE
            )

        try:
            svg_target = create_per_rpc_svg_target(stats, self.rpc_id_dict, rpc_list)
        except Exception as e:
            svg_target = pn.pane.Markdown(
                f"{str(e)}",
                width=450,
                styles=HIGHLIGHT_STYLE
            )

        """Create the detailed analysis layout with user-friendly explanations"""
        return pn.Column(
            pn.pane.Markdown("## Detailed Performance Analysis", styles=SECTION_STYLE),
            pn.pane.Markdown(
                f"Analyzing **{len(rpc_list)} unique RPC call(s)**. Here's what we found:",
                styles=DESCRIPTION_STYLE
            ),
            
            # Timing Breakdown Section (server-side)
            pn.pane.Markdown("### Timing Breakdown (Server Side)", styles=SUB_SECTION_STYLE),
            pn.pane.Markdown(
                "Let's start by looking at the big picture. Each chord shows a connection between a client and a server. The color of the chord tells you which client made the request, and the thickness shows how much time the server spent working on it- the thicker the chord, the more work was done.",
                styles=DESCRIPTION_STYLE
            ),   
            chord_graph_server_view,
            pn.pane.Markdown(
                "Are there any unusually thick connections between processes? You can go back to Step 1 to cherry-pick those processes to narrow it down later.",
                styles=DESCRIPTION_STYLE,
            ),
            pn.pane.Markdown(
                "Now, let's zoom in and start by looking at the performance of your selected RPCs in the server side. Which RPCs are taking the most time to execute?",
                styles=DESCRIPTION_STYLE
            ),     
            graph_6_view,
            pn.pane.Markdown(
                get_graph_6_description(),
                styles=DESCRIPTION_STYLE
            ),

            # Timing Breakdown Section (client-side)
            pn.pane.Markdown("### Timing Breakdown (Client Side)", styles=SUB_SECTION_STYLE),
            pn.pane.Markdown(
                "Now let's switch to the client's perspective. Thicker chords mean the client spent more time on their requests.",
                styles=DESCRIPTION_STYLE
            ), 
            chord_graph_client_view,
            pn.pane.Markdown(
                "Are there any unusually thick connections between processes? Again, you can go back to Step 1 to cherry-pick those processes and narrow it down later.",
                styles=DESCRIPTION_STYLE,
            ),
            pn.pane.Markdown(
                "Now, let's zoom in to the RPCs and look at the performance of your selected RPCs in the client side. Which RPCs are taking the most time on the request?",
                styles=DESCRIPTION_STYLE
            ),     
            pn.Column(
                graph_7_view,
                pn.pane.Markdown(
                    get_graph_7_description(),
                    styles=DESCRIPTION_STYLE
                ),
            ),

            # Workload Distribution Section
            pn.pane.Markdown("### Calls Distribution", styles=SUB_SECTION_STYLE),
            pn.pane.Markdown(
                "How is the number of calls distributed across your processes? This helps you understand load balancing and identify potential bottlenecks.",
                styles=DESCRIPTION_STYLE
            ),
            self._create_distribution_view(stats, rpc_list),

            # Remote Procedure Call Section
            pn.pane.Markdown("### Understanding RPC Lifecycle", styles=SUB_SECTION_STYLE),
            pn.pane.Markdown(
                "RPCs go through several stages from initiation to completion. The diagrams below show the typical flow:",
                styles=DESCRIPTION_STYLE
            ),
            pn.Row(
                pn.Column(
                    pn.pane.Markdown(
                        "**Client Side (Left):** Initiates the RPC call and waits for response",
                        width=300,
                        styles=DESCRIPTION_STYLE
                    ),
                    pn.pane.Markdown(
                        "**Server Side (Right):** Receives the request, processes it, and sends back results",
                        width=300,
                        styles=DESCRIPTION_STYLE
                    ),
                    pn.pane.Markdown(
                        "This flow helps you understand where time is being spent in your system.",
                        width=300,
                        styles=DESCRIPTION_STYLE
                    ),
                ),
                pn.pane.SVG("./img/rpc-origin.svg", width=300, height=400),
                pn.pane.SVG("./img/rpc-target.svg", width=300, height=400),
            ),
            
            pn.pane.Markdown("### Your RPCs in Detail", styles=SUB_SECTION_STYLE),
            pn.pane.Markdown(
                "Now let's dive even deeper and see the steps for all the selected RPCs. These visualizations show the scaled timing of each step in your selected RPCs, helping you identify exactly where bottlenecks occur in your workflow.",
                styles=DESCRIPTION_STYLE
            ),
            pn.Row(
                pn.Column(
                    pn.pane.Markdown("#### Client-Side RPC Steps", styles=SUB_SECTION_STYLE),
                    pn.pane.Markdown(
                        "This visualization shows the timing breakdown of client-side steps for your selected RPCs. The segments show how the average time was spent in each phase of the client-side process.",
                        width=450,
                        styles=DESCRIPTION_STYLE
                    ),
                    svg_origin,
                ),
                pn.Column(
                    pn.pane.Markdown("#### Server-Side RPC Steps", styles=SUB_SECTION_STYLE),
                    pn.pane.Markdown(
                        "This visualization shows the timing breakdown of server-side steps for your selected RPCs. The segments show how the average time was spent in each phase of the server-side process.",
                        width=450,
                        styles=DESCRIPTION_STYLE
                    ),
                    svg_target,
                ),
            ),

            pn.pane.Markdown(
                """
                Do you notice anything unusual? Watch for long set/get_input or set/get_output times- this means heavy data serialization, so consider bulk transfers. If iforward_wait is long but starts right after forwarding ends, it's likely a blocking call- try overlapping computation. If ult.relative_timestamp_from_handler_start is high or inconsistent, you may have a scheduling delay- more threads or servers can help.
                """,
                styles=DESCRIPTION_STYLE
            ),

            pn.pane.Markdown("### Where is Time Being Spent?", styles=SUB_SECTION_STYLE),
            pn.pane.Markdown(
                "This breakdown shows exactly where time is being consumed in your RPC workflow. Look for the tallest bars - those are your bottlenecks!",
                styles=DESCRIPTION_STYLE
            ),
            pn.pane.Markdown(
                "**Legend:** <span style='color:#1f77b4'>🔵 Client steps</span> &nbsp;&nbsp; <span style='color:#ff7f0e'>🟠 Server steps</span>", 
                styles=DESCRIPTION_STYLE
            ),
            graph_8_view,
            pn.pane.Markdown(
                get_graph_8_description(),
                styles=DESCRIPTION_STYLE
            ),

            pn.pane.Markdown("### Performance Variability", styles=SUB_SECTION_STYLE),
            pn.pane.Markdown(
                "These charts show which functions have the most unpredictable performance. "
                "High variability (large error bars) might indicate issues that need attention.",
                styles=DESCRIPTION_STYLE
            ),
            graph_9_view,
            graph_10_view,
            
            pn.pane.Markdown(
                "**Analysis Tips:** Look for patterns in the data. Are certain steps consistently slow? "
                "Do some RPCs show much more variability than others? These insights can guide your optimization efforts.",
                styles=TIP_STYLE
            ),
            styles=BORDER_STYLE
        ) 
   
    def _create_distribution_view(self, stats, rpc_list):
        """Create distribution view with user-friendly explanations"""
        try:
            client_heatmap = create_rpc_load_heatmap(stats, self.rpc_id_dict, rpc_list, 'clients')
        except Exception as e:
            client_heatmap = pn.pane.Markdown(f"{str(e)}", styles=HIGHLIGHT_STYLE)
        try:
            server_heatmap = create_rpc_load_heatmap(stats, self.rpc_id_dict, rpc_list, 'servers')
        except Exception as e:
            server_heatmap = pn.pane.Markdown(f"{str(e)}", styles=HIGHLIGHT_STYLE)
        return pn.Column(
            pn.Column(
                pn.pane.Markdown("### Who's Sending the Most Requests?", styles=SUB_SECTION_STYLE),
                pn.pane.Markdown(
                    "This heatmap shows which client processes are making the most RPC calls.",
                    styles=DESCRIPTION_STYLE
                ),
                client_heatmap,
                pn.pane.Markdown(
                    get_heatmap_description(view_type='clients'),
                    styles=DESCRIPTION_STYLE
                ),
            ),
            pn.Column(
                pn.pane.Markdown("### Who's Handling the Most Work?", styles=SUB_SECTION_STYLE),
                pn.pane.Markdown(
                    "This heatmap shows which server processes are handling the most RPC requests. Look for potential overloaded servers.",
                    styles=DESCRIPTION_STYLE
                ),
                server_heatmap,
                pn.pane.Markdown(
                    get_heatmap_description(view_type='servers'),
                    styles=DESCRIPTION_STYLE
                ),
            )
        )