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

COMMON COMPONENTS:
==================
    - MochiDashboard: Main dashboard class
    - _create_header, _create_overview_section, etc.: Section builder methods
    - Panel widgets: Dropdowns, buttons, tables for interactivity
    - Styling dicts: For consistent look and feel
"""
import panel as pn
import pandas as pd

from holoviews.streams import Tap
from plotting_functions import MochiPlotter

TITLE_STYLE = {
    'font-size': '28px',
    'font-weight': 'bold',
    'color': '#2c3e50',
    'text-align': 'center'
}

SECTION_STYLE = {
    'font-size': '22px',
    'font-weight': '600',
    'color': '#34495e',
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
}

DESCRIPTION_STYLE = {
    "font-size": "15px",
    "color": "#555",
    "line-height": "1.6",
    "white-space": "pre-wrap", 
    "overflow-wrap": "break-word",
    "max-width": "1000px", 
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
    'border-radius': '8px',
    'border-left': '4px solid #e74c3c',
    'font-size': '14px',
    "max-width": "1000px",
    'color': '#2c3e50'
}

TIP_STYLE = {
    'background': '#e8f4fd',
    'padding': '15px',
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
}
MAIN_PAGE_WIDTH = 1100
pn.extension('tabulator')

class MochiDashboard():
    def __init__(self, stats):
        self.plotter = MochiPlotter(stats)
        # Create main dashboard with improved flow
        main_page = pn.Column(
            self._create_header(),
            self._create_overview_section(),
            self._create_process_analysis_section(),
            self._create_detailed_analysis_section(),
            self._create_advanced_analysis_section(),
            styles={'max-width': '1200px'}
        )
        # Navigation system
        self.trigger = pn.widgets.TextInput(name='Page Toggle', value='', visible=False)
        @pn.depends(self.trigger)
        def get_page(context):
            if not context or context == 'back_to_main_page':
                print("Attempting to display main_page")
                return main_page
            else:
                print("Attempting to display PER-RPC page")
                return self._create_per_rpc_view()
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

    def _create_overview_section(self):
        """Create high-level system overview with user-friendly explanations"""
        try:
            graph_1_view = self.plotter.create_graph_1()
        except Exception as e:
            graph_1_view = pn.pane.Markdown(
                f"{str(e)}",
                styles=HIGHLIGHT_STYLE
            )
        try:
            graph_2_view = self.plotter.create_graph_2()
        except Exception as e:
            graph_2_view = pn.pane.Markdown(
                f"{str(e)}",
                styles=HIGHLIGHT_STYLE
            )
        return pn.Column(
            pn.pane.Markdown("## System Overview", styles=SECTION_STYLE),
            pn.pane.Markdown(
                "Let's start with the big picture. These charts show you which processes are doing the most work "
                "and how your system is communicating overall.",
                styles=DESCRIPTION_STYLE
            ),
            pn.Column(
                graph_1_view,
                pn.pane.Markdown(
                    "**What this shows:** See which processes are the most active clients in your system. Higher bars mean a process is making more RPC calls to others. Use this to spot your busiest clients.",
                    styles=DESCRIPTION_STYLE
                ),
            ),
            pn.Column(
                graph_2_view,
                pn.pane.Markdown(
                    "**What this shows:** Find out which processes are doing the most work as servers. Higher bars mean a process is handling more RPC requests. This helps you spot overloaded servers.",
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

    def _create_process_analysis_section(self):
        """Create interactive process analysis with guided exploration"""
        process_dropdown = pn.widgets.Select(
            name='Select a Process to Analyze', 
            options=self.plotter.get_all_addresses(),
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
                client_view = self.plotter.create_graph_3(process_choice)
            except:
                client_view = pn.pane.Markdown(
                    f"**No data available for {process_choice}** - This process may not have made any RPC calls.",
                    styles=HIGHLIGHT_STYLE
                )
            try:
                server_view = self.plotter.create_graph_4(process_choice)
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
                        "**What this shows:** For the selected process, see how much time it spends calling each type of RPC. This helps you understand what kinds of work your client is doing most.",
                        styles=DESCRIPTION_STYLE
                    ),
                ),
                pn.Column(
                    pn.pane.Markdown("**As a Server** (RPCs it handles)", styles=SUB_SECTION_STYLE),
                    server_view,
                    pn.pane.Markdown(
                        "**What this shows:** For the selected process, see how much time it spends handling each type of incoming RPC. This reveals what kinds of requests your server is working on most.",
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

    def _create_detailed_analysis_section(self):
        metric_dropdown = pn.widgets.Select(
            name='Performance Metric', 
            options=['Server Execution Time', 'Client Call Time'], 
            value='Server Execution Time',
            width=300
        )

        @pn.depends(metric_dropdown)
        def get_visualization(metric_choice):
            try:
                graph_5_view = self.plotter.create_graph_5(metric_choice)
                # Only create the Tap stream if we have a valid plot
                bars = graph_5_view
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
                    "**What this shows:** These are the top 5 RPCs using the most resources, based on your selected metric. Use this to quickly find which RPCs are slowing things down or using the most bandwidth. Click any bar to explore detailed statistics for that specific RPC call.",
                        styles=DESCRIPTION_STYLE
                    )
                )
            except Exception as e:
                # If graph creation fails, just return the error message without interactive features
                return pn.Column(
                    pn.pane.Markdown(
                        f"{str(e)}",
                        styles=HIGHLIGHT_STYLE
                    ),
                    pn.pane.Markdown(
                    "**What this shows:** These are the top 5 RPCs using the most resources, based on your selected metric. Use this to quickly find which RPCs are slowing things down or using the most bandwidth.",
                        styles=DESCRIPTION_STYLE
                    )
                )
        
        return pn.Column(
            pn.pane.Markdown("## RPC Performance Analysis", styles=SECTION_STYLE),
            pn.pane.Markdown(
                "Here you can see which RPC calls are using the most resources in your system. "
                "Pick a metric (like execution time or client call time) to see which RPC is demanding for the clients and servers.",
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

    def _create_advanced_analysis_section(self):
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
    def _create_per_rpc_view(self):
        """Create the detailed RPC view with improved user guidance"""
        # Define components
        all_options = self.plotter.get_all_addresses()
        origin_search = pn.widgets.TextInput(
            name='Search Source Processes',
            placeholder='Type to filter source processes...',
            width=300
        )
        target_search = pn.widgets.TextInput(
            name='Search Destination Processes', 
            placeholder='Type to filter destination processes...',
            width=300
        )
        origin_count_label = pn.pane.Markdown(
            f"**{len(all_options)}** processes available",
            styles=DESCRIPTION_STYLE,
            width=300
        )
        target_count_label = pn.pane.Markdown(
            f"**{len(all_options)}** processes available", 
            styles=DESCRIPTION_STYLE,
            width=300
        )
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
        # Quick filter buttons for each process selection area
        origin_clear_btn = pn.widgets.Button(
            name='Clear Selection', 
            button_type='default', 
            width=110, 
            height=35
        )
        origin_select_all_btn = pn.widgets.Button(
            name='Select All Visible', 
            button_type='default', 
            width=110, 
            height=35
        )
        target_clear_btn = pn.widgets.Button(
            name='Clear Selection', 
            button_type='default', 
            width=110, 
            height=35
        )
        target_select_all_btn = pn.widgets.Button(
            name='Select All Visible', 
            button_type='default', 
            width=110, 
            height=35
        )
        rpc_table_wrapper = pn.Column()
        right_layout = pn.Column()
        self.src_files, self.dest_files = [], []
        self.tab_selection = []
        self.current_rpc_df = None  
        # Track hidden selections separately
        self.origin_hidden_selections = []
        self.target_hidden_selections = []
        self.rpc_hidden_selections = []  
    
        def update_origin_options(search_term):
            filtered_options = [opt for opt in all_options if search_term.lower() in opt.lower()] if search_term else all_options
            current_selections = origin_select.value if origin_select.value else []
            all_current_selections = list(set(current_selections + self.origin_hidden_selections))
            origin_select.options = filtered_options
            visible_selections = [s for s in all_current_selections if s in filtered_options]
            self.origin_hidden_selections = [s for s in all_current_selections if s not in filtered_options]
            origin_select.value = visible_selections
            total_selected = len(all_current_selections)
            hidden_count = len(self.origin_hidden_selections)
            if hidden_count > 0:
                origin_count_label.object = f"**{total_selected}** selected ({hidden_count} hidden), **{len(filtered_options)}** of **{len(all_options)}** processes shown"
            else:
                origin_count_label.object = f"**{total_selected}** selected, **{len(filtered_options)}** of **{len(all_options)}** processes shown"
        
        def update_target_options(search_term):
            filtered_options = [opt for opt in all_options if search_term.lower() in opt.lower()] if search_term else all_options
            current_selections = target_select.value if target_select.value else []
            all_current_selections = list(set(current_selections + self.target_hidden_selections))
            target_select.options = filtered_options
            visible_selections = [s for s in all_current_selections if s in filtered_options]
            self.target_hidden_selections = [s for s in all_current_selections if s not in filtered_options]
            target_select.value = visible_selections
            total_selected = len(all_current_selections)
            hidden_count = len(self.target_hidden_selections)
            if hidden_count > 0:
                target_count_label.object = f"**{total_selected}** selected ({hidden_count} hidden), **{len(filtered_options)}** of **{len(all_options)}** processes shown"
            else:
                target_count_label.object = f"**{total_selected}** selected, **{len(filtered_options)}** of **{len(all_options)}** processes shown"

        def on_back_button_click(event):
            self.trigger.value = 'back_to_main_page'

        def origin_on_change(event):
            # Merge visible selections with hidden ones for the complete selection list
            visible_selections = event.new if event.new else []
            all_selections = list(set(visible_selections + self.origin_hidden_selections))
            self.src_files = all_selections
            selected_count = len(all_selections)
            total_shown = len(origin_select.options)
            hidden_count = len(self.origin_hidden_selections)
            # Update display
            if hidden_count > 0:
                origin_count_label.object = f"**{selected_count}** selected ({hidden_count} hidden), **{total_shown}** of **{len(all_options)}** processes shown"
            else:
                origin_count_label.object = f"**{selected_count}** selected, **{total_shown}** of **{len(all_options)}** processes shown"
                
        def target_on_change(event):
            # Merge visible selections with hidden ones for the complete selection list
            visible_selections = event.new if event.new else []
            all_selections = list(set(visible_selections + self.target_hidden_selections))
            self.dest_files = all_selections
            selected_count = len(all_selections)
            total_shown = len(target_select.options)
            hidden_count = len(self.target_hidden_selections)
            # Update display
            if hidden_count > 0:
                target_count_label.object = f"**{selected_count}** selected ({hidden_count} hidden), **{total_shown}** of **{len(all_options)}** processes shown"
            else:
                target_count_label.object = f"**{selected_count}** selected, **{total_shown}** of **{len(all_options)}** processes shown"

        def tabulator_selection_on_change(event):
            # This will be redefined inside on_apply_button_click where df is available
            self.tab_selection = event.new

        def on_origin_search_change(event):
            update_origin_options(event.new)

        def on_target_search_change(event):
            update_target_options(event.new)

        def on_origin_clear_click(event):
            origin_search.value = ''
            origin_select.value = []
            self.origin_hidden_selections = []  # Clear hidden selections too
            self.src_files = []
            # Reset the count label to show all processes
            origin_count_label.object = f"**0** selected, **{len(all_options)}** of **{len(all_options)}** processes shown"

        def on_origin_select_all_click(event):
            # Select all visible options and clear hidden selections since we're selecting all visible
            origin_select.value = origin_select.options
            self.origin_hidden_selections = []

        def on_target_clear_click(event):
            target_search.value = ''
            target_select.value = []
            self.target_hidden_selections = []  # Clear hidden selections too
            self.dest_files = []
            # Reset the count label to show all processes
            target_count_label.object = f"**0** selected, **{len(all_options)}** of **{len(all_options)}** processes shown"

        def on_target_select_all_click(event):
            # Select all visible options and clear hidden selections since we're selecting all visible
            target_select.value = target_select.options
            self.target_hidden_selections = []

        def on_tabulation_confirm_view_click(event):
            # This will be redefined inside on_apply_button_click where df is available
            if not self.tab_selection:
                return
            # Default behavior for when no table is loaded yet
            right_layout.clear()
            right_layout.append(pn.pane.Markdown("Please select processes and apply to see RPCs first.", styles=DESCRIPTION_STYLE))

        def on_apply_button_click(event):
            if not self.src_files or not self.dest_files:
                # Show helpful message when no processes are selected
                rpc_table_wrapper.clear()
                rpc_table_wrapper.append(
                    pn.Column(
                        pn.pane.Markdown("### Step 2: Select Source and Destination Processes", styles=SUB_SECTION_STYLE),
                        pn.pane.Markdown(
                            "**Please select at least one source process and one destination process** to see available RPC communications.",
                            styles=HIGHLIGHT_STYLE
                        ),
                        pn.pane.Markdown(
                            "**Tips:**\n• Use the search boxes to find specific processes quickly\n• Select multiple processes to see broader communication patterns\n• Use 'Select All Visible' to select all currently shown processes",
                            styles=TIP_STYLE
                        ),
                        styles=BORDER_STYLE,
                        width=700
                    )
                )
                return
            # Create table with better styling
            df = self.plotter.create_rpc_table_dataframe(self.src_files, self.dest_files)
            # Reset hidden selections when creating a new RPC table
            self.rpc_hidden_selections = []
            # RPC search functionality
            rpc_search = pn.widgets.TextInput(
                name='Search RPCs',
                placeholder='Type to filter RPC calls...',  
                width=400
            )
            # RPC count indicator  
            rpc_count_label = pn.pane.Markdown(
                f"**{len(df)}** RPCs shown",
                styles=DESCRIPTION_STYLE
            )
            # Create filtered table
            filtered_df = df.copy()
            self.current_rpc_df = filtered_df  # Initialize instance variable
            table = pn.widgets.Tabulator(filtered_df, selectable=True, disabled=True, configuration={'selectableRowsRangeMode': 'click'})
            
            def update_rpc_table(search_term):
                nonlocal filtered_df
                # Get current complete selections (visible + hidden)
                current_selections = table.selection if table.selection else []
                # Convert current visible selections to actual row indices from the original dataframe
                all_current_selections = []
                for visible_idx in current_selections:
                    if visible_idx < len(filtered_df):
                        # Find this row in the original dataframe
                        filtered_row = filtered_df.iloc[visible_idx]
                        original_idx = df[(df['Source'] == filtered_row['Source']) & 
                                        (df['Target'] == filtered_row['Target']) & 
                                        (df['RPC'] == filtered_row['RPC'])].index[0]
                        all_current_selections.append(original_idx)
                # Add hidden selections
                all_current_selections.extend(self.rpc_hidden_selections)
                all_current_selections = list(set(all_current_selections))  # Remove duplicates
                # Apply filter
                if search_term:
                    filtered_df = df[df['RPC'].str.contains(search_term, case=False, na=False)]
                else:
                    filtered_df = df.copy()
                # Update table
                table.value = filtered_df
                self.current_rpc_df = filtered_df  # Store in instance variable
                # Separate visible and hidden selections
                visible_selections = []
                hidden_selections = []
                for original_idx in all_current_selections:
                    if original_idx < len(df):
                        original_row = df.iloc[original_idx]
                        # Check if this row exists in filtered results
                        matching_rows = filtered_df[(filtered_df['Source'] == original_row['Source']) & 
                                                   (filtered_df['Target'] == original_row['Target']) & 
                                                   (filtered_df['RPC'] == original_row['RPC'])]
                        if len(matching_rows) > 0:
                            # Row is visible, find its index in filtered_df
                            filtered_idx = matching_rows.index[0]
                            # Convert to position in filtered_df
                            visible_idx = filtered_df.index.get_loc(filtered_idx)
                            visible_selections.append(visible_idx)
                        else:
                            # Row is hidden
                            hidden_selections.append(original_idx)
                # Update selections
                table.selection = visible_selections
                self.rpc_hidden_selections = hidden_selections
                # Update display
                total_selected = len(all_current_selections)
                hidden_count = len(hidden_selections)
                if hidden_count > 0:
                    rpc_count_label.object = f"**{total_selected}** selected ({hidden_count} hidden), **{len(filtered_df)}** of **{len(df)}** RPCs shown"
                else:
                    rpc_count_label.object = f"**{total_selected}** selected, **{len(filtered_df)}** of **{len(df)}** RPCs shown"
            
            def on_rpc_search_change(event):
                update_rpc_table(event.new)
            
            rpc_search.param.watch(on_rpc_search_change, 'value')
            
            # Add select all and clear buttons
            select_all_button = pn.widgets.Button(
                name='Select All Visible', 
                button_type='default',
                width=120
            )
            clear_rpc_button = pn.widgets.Button(
                name='Clear Selection', 
                button_type='default',
                width=120
            )
            def on_select_all_click(event):
                # Select all rows in the filtered table and clear hidden selections since we're selecting all visible
                table.selection = list(range(len(filtered_df)))
                self.rpc_hidden_selections = []
            
            def on_clear_rpc_click(event):
                # Clear all selections, both visible and hidden
                table.selection = []
                self.rpc_hidden_selections = []
                
            # Redefine tabulator selection change handler with access to df and rpc_count_label
            def table_selection_change(event):
                # Convert visible selections to original dataframe indices and merge with hidden selections
                visible_selections = event.new if event.new else []
                original_indices = []
                
                # Convert visible selections to original dataframe indices
                for visible_idx in visible_selections:
                    if visible_idx < len(filtered_df):
                        filtered_row = filtered_df.iloc[visible_idx]
                        # Find this row in the original dataframe
                        matching_rows = df[(df['Source'] == filtered_row['Source']) & 
                                         (df['Target'] == filtered_row['Target']) & 
                                         (df['RPC'] == filtered_row['RPC'])]
                        if len(matching_rows) > 0:
                            original_indices.append(matching_rows.index[0])
                
                # Merge with hidden selections and store complete selection
                all_selections = list(set(original_indices + self.rpc_hidden_selections))
                self.tab_selection = all_selections
                
                # Update count display
                total_selected = len(all_selections)
                hidden_count = len(self.rpc_hidden_selections)
                total_shown = len(filtered_df)
                
                if hidden_count > 0:
                    rpc_count_label.object = f"**{total_selected}** selected ({hidden_count} hidden), **{total_shown}** of **{len(df)}** RPCs shown"
                else:
                    rpc_count_label.object = f"**{total_selected}** selected, **{total_shown}** of **{len(df)}** RPCs shown"
            
            # Redefine confirm view handler with access to df
            def confirm_view_click(event):
                if not self.tab_selection:
                    return
                print("Creating graphs...")
                rpc_list = []
                # tab_selection now contains original dataframe indices
                for original_idx in self.tab_selection:
                    if original_idx < len(df):
                        src_address = df.iloc[original_idx]['Source']
                        dst_address = df.iloc[original_idx]['Target'] 
                        RPC = df.iloc[original_idx]['RPC']
                        rpc_list.append((src_address, dst_address, RPC))
                right_layout.clear()
                # Add loading indicator
                loading_indicator = pn.indicators.LoadingSpinner(value=True, width=50, height=50, color='primary')
                right_layout.append(pn.Column(loading_indicator, pn.pane.Markdown("""Loading detailed analysis... (this may take a while)\n\nFirst run? It can take longer as we build the cache. Subsequent runs will be faster.""", styles=DESCRIPTION_STYLE)))
                # This method will take a while
                detailed_rpc_layout = self._create_detailed_per_rpc_layout(rpc_list)            
                right_layout.clear()
                print("Rendering graphs...")
                right_layout.append(detailed_rpc_layout)
                
            select_all_button.on_click(on_select_all_click)
            clear_rpc_button.on_click(on_clear_rpc_click)
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
                        f"Found **{len(df)}** RPC communication(s) between your selected processes. Use the search box to filter and choose which ones you want to analyze in detail: ",
                        styles=DESCRIPTION_STYLE
                    ),
                    rpc_search,
                    rpc_count_label,
                    pn.Row(clear_rpc_button, pn.Spacer(width=20), select_all_button),
                    table,
                    confirm_button,
                    pn.pane.Markdown(
                        "**Selection Tips:** Click to select individual rows, Ctrl+click for multiple selections, or Shift+click for ranges. Use the search box above to filter by RPC name. Your selections are preserved when filtering - even hidden RPCs remain selected.",
                        styles=TIP_STYLE
                    ),
                    pn.pane.Markdown('*Note: You may notice provider IDs are included in the logs. In our case, RPC grouping ignores provider ID and parent provider ID for simplified analysis*',styles=SIDE_NOTE_STYLE),  
                    styles=BORDER_STYLE,
                    width=700
                )
            )
            table.param.watch(table_selection_change, 'selection')
            confirm_button.on_click(confirm_view_click)

        origin_select.param.watch(origin_on_change, 'value')
        target_select.param.watch(target_on_change, 'value')
        origin_search.param.watch(on_origin_search_change, 'value')
        target_search.param.watch(on_target_search_change, 'value')
        origin_clear_btn.on_click(on_origin_clear_click)
        origin_select_all_btn.on_click(on_origin_select_all_click)
        target_clear_btn.on_click(on_target_clear_click)
        target_select_all_btn.on_click(on_target_select_all_click)
        apply_button.on_click(on_apply_button_click)
        back_button.on_click(on_back_button_click)
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
                    pn.pane.Markdown(
                        "• **Source**: Processes that initiate RPC calls (clients)\n• **Destination**: Processes that receive and handle RPC calls (servers)\n• **Tip**: Use the search boxes to quickly find specific processes, or use quick filters below",
                        styles=DESCRIPTION_STYLE
                    ),
                    pn.Row(
                        pn.Column(
                            origin_search,
                            origin_count_label,
                            pn.Row(origin_clear_btn, pn.Spacer(width=20), origin_select_all_btn),
                            origin_select,
                            width=300
                        ),
                        pn.Column(
                            target_search,
                            target_count_label,
                            pn.Row(target_clear_btn, pn.Spacer(width=20), target_select_all_btn),
                            target_select,
                            width=300
                        ),  
                    ),
                    apply_button,
                    pn.pane.Markdown(
                        "**Search Tip:** Your selections are preserved when filtering - even if they're hidden from view, they'll still be used when you click Apply. Hidden selections are shown in the count above.",
                        styles=TIP_STYLE
                    ),
                    styles=BORDER_STYLE,
                    width=700
                ),
                rpc_table_wrapper,
            ),
            pn.Column(
                right_layout,
                pn.Row(back_button),
                styles={'max-width': '1400px'}
            ),
        )

    def _create_detailed_per_rpc_layout(self, rpc_list):
        """Create the detailed analysis layout with user-friendly explanations"""
        return pn.Column(
            pn.pane.Markdown("## Detailed Performance Analysis", styles=SECTION_STYLE),
            pn.pane.Markdown(
                f"Analyzing **{len(rpc_list)} unique RPC call(s)**. Here's what we found:",
                styles=DESCRIPTION_STYLE
            ),
            *self._create_detailed_per_rpc_server_layout(rpc_list),
            *self._create_detailed_per_rpc_client_layout(rpc_list),
            styles=BORDER_STYLE
        ) 
    
    def _create_detailed_per_rpc_server_layout(self, rpc_list):
        try:
            server_heatmap = self.plotter.create_rpc_load_heatmap(rpc_list, 'servers')
        except Exception as e:
            server_heatmap = pn.pane.Markdown(
                f"{str(e)}", 
                styles=HIGHLIGHT_STYLE
            )
        try:
            chord_graph_server_view = self.plotter.create_chord_graph(rpc_list, view_type='servers')
        except Exception as e:
            chord_graph_server_view = pn.pane.Markdown(
                f"{str(e)}",
                styles=HIGHLIGHT_STYLE
            )
        try:
            graph_6_view = self.plotter.create_graph_6(rpc_list)
        except Exception as e:
            graph_6_view = pn.pane.Markdown(
                f"{str(e)}",
                styles=HIGHLIGHT_STYLE
            )
        try:
            graph_8_view = self.plotter.create_graph_8(rpc_list, view_type='servers')
        except Exception as e:
            graph_8_view = pn.pane.Markdown(
                f"{str(e)}",
                styles=HIGHLIGHT_STYLE
            )
        try:
            graph_9_view = self.plotter.create_graph_9(rpc_list)
        except Exception as e:
            graph_9_view = pn.pane.Markdown(
                f"{str(e)}",
                styles=HIGHLIGHT_STYLE
            )
        try:
            svg_target = self.plotter.create_per_rpc_svg_target(rpc_list)
        except Exception as e:
            svg_target = pn.pane.Markdown(
                f"{str(e)}",
                width=450,
                styles=HIGHLIGHT_STYLE
            )
        return [
            # Let's examine the server-side first!
            pn.pane.Markdown(
                "## Server Analysis: Understanding Performance Bottlenecks",
                styles=SUB_SECTION_STYLE
            ),
            pn.pane.Markdown(
                "Now let's dive into the server-side perspective to understand where time is being spent processing requests. This analysis will help you identify which RPCs are consuming the most resources, how workload is distributed across your servers, and where potential bottlenecks might be occurring.",
                styles=DESCRIPTION_STYLE
            ), 
            # Timing Breakdown Section (server-side)
            pn.pane.Markdown("### Timing Breakdown", styles=SUB_SECTION_STYLE),
            pn.pane.Markdown(
                "Let's start by looking at the big picture. Each chord shows a connection between a client and a server. The color of the chord tells you which client made the request, and the thickness shows how much time the server spent working on it- the thicker the chord, the more work was done.",
                styles=DESCRIPTION_STYLE
            ),   
            pn.Column(chord_graph_server_view, width=700, height=700),
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
                "**What this shows:** These are the top 5 server-side RPCs with the highest average execution time. For each, you can see the max, average, and min times. Use this to find slow server operations to optimize.",
                styles=DESCRIPTION_STYLE
            ),
            # Workload Distribution (server-side)
            pn.pane.Markdown(
                "How is the number of calls distributed across your processes? This helps you understand load balancing and identify potential bottlenecks.",
                styles=DESCRIPTION_STYLE
            ),
            pn.pane.Markdown("### Who's Handling the Most Work?", styles=SUB_SECTION_STYLE),
            pn.pane.Markdown(
                "This heatmap shows which server processes are handling the most RPC requests. Look for potential overloaded servers.",
                styles=DESCRIPTION_STYLE
            ),
            server_heatmap,
            pn.pane.Markdown(
                "**What this shows:** This heatmap shows which server processes are handling the most RPC requests and which RPC types are most common. Look for hotspots to find overloaded servers or popular RPCs.",
                styles=DESCRIPTION_STYLE
            ),
            # Remote Procedure Call (server-side)
            pn.pane.Markdown("### Understanding the RPC Lifecycle", styles=SUB_SECTION_STYLE),
            pn.pane.Markdown(
                "RPCs go through several stages from initiation to completion. The diagram below shows the typical flow of the server RPC lifecycle:",
                styles=DESCRIPTION_STYLE
            ),
            pn.Row(
                pn.Column(
                    pn.pane.Markdown(
                        "**Server Side:** Receives the request, processes it, and sends back results",
                        width=300,
                        styles=DESCRIPTION_STYLE
                    ),
                    pn.pane.Markdown(
                        "This flow helps you understand where time is being spent in your system.",
                        width=300,
                        styles=DESCRIPTION_STYLE
                    ),
                ),
                pn.pane.SVG("./img/rpc-target.svg", width=300, height=400),
            ),
            pn.pane.Markdown("### Your RPCs in Detail", styles=SUB_SECTION_STYLE),
            pn.pane.Markdown(
                "Now let's dive even deeper and see the steps for all the selected RPCs. These visualizations show the scaled timing of each step in your selected RPCs, helping you identify exactly where bottlenecks occur in your workflow.",
                styles=DESCRIPTION_STYLE
            ),
            pn.pane.Markdown(
                "This visualization shows the timing breakdown of server-side steps for your selected RPCs. The segments show how the average time was spent in each phase of the server-side process.",
                styles=DESCRIPTION_STYLE
            ),
            pn.Row(
                pn.pane.Markdown(
                    """
                    Do you notice anything unusual? Watch for long get_input or set_output times- this means heavy data serialization, so consider bulk transfers. If ult.relative_timestamp_from_handler_start is high or inconsistent, you may have a scheduling delay- more threads or servers can help.
                    """,
                    width=400,
                    styles=DESCRIPTION_STYLE
                ),
                svg_target,
            ),
            pn.pane.Markdown("### Where is Time Being Spent?", styles=SUB_SECTION_STYLE),
            pn.pane.Markdown(
                "This breakdown shows exactly where time is being consumed in your RPC workflow. Look for the tallest bars - those are your bottlenecks!",
                styles=DESCRIPTION_STYLE
            ),
            graph_8_view,
            pn.pane.Markdown(
                "**What this shows:** See how much total time is spent in each step of the RPC process, across all selected RPCs. Taller bars mean more time spent in that step. Focus on the tallest bars to find bottlenecks.",
                styles=DESCRIPTION_STYLE
            ),
            pn.pane.Markdown("### Performance Variability", styles=SUB_SECTION_STYLE),
            pn.pane.Markdown(
                "This chart show which functions have the most unpredictable performance. "
                "High variability (large error bars) might indicate issues that need attention.",
                styles=DESCRIPTION_STYLE
            ),
            graph_9_view,
            pn.pane.Markdown(
                "**Analysis Tips:** Look for patterns in the data. Are certain steps consistently slow? "
                "Do some RPCs show much more variability than others? These insights can guide your optimization efforts.",
                styles=TIP_STYLE
            ),
        ]

    def _create_detailed_per_rpc_client_layout(self, rpc_list):
        try:
            client_heatmap = self.plotter.create_rpc_load_heatmap(rpc_list, 'clients')
        except Exception as e:
            client_heatmap = pn.pane.Markdown(
                f"{str(e)}", 
                styles=HIGHLIGHT_STYLE
            )
        try:    
            chord_graph_client_view = self.plotter.create_chord_graph(rpc_list, view_type='clients')
        except Exception as e:
            chord_graph_client_view = pn.pane.Markdown(
                f"{str(e)}",
                styles=HIGHLIGHT_STYLE
            )
        try:
            graph_7_view = self.plotter.create_graph_7(rpc_list)
        except Exception as e:
            graph_7_view = pn.pane.Markdown(
                f"{str(e)}",
                styles=HIGHLIGHT_STYLE
            )
        try:
            graph_8_view = self.plotter.create_graph_8(rpc_list, view_type='clients')
        except Exception as e:
            graph_8_view = pn.pane.Markdown(
                f"{str(e)}",
                styles=HIGHLIGHT_STYLE
            )
        try:
            graph_10_view = self.plotter.create_graph_10(rpc_list)
        except Exception as e:
            graph_10_view = pn.pane.Markdown(
                f"{str(e)}",
                styles=HIGHLIGHT_STYLE
            )
        try:
            svg_origin = self.plotter.create_per_rpc_svg_origin(rpc_list)
        except Exception as e:
            svg_origin = pn.pane.Markdown(
                f"{str(e)}",
                width=450,
                styles=HIGHLIGHT_STYLE
            )
        return [
            # Let's examine the client-side first!
            pn.pane.Markdown(
                "## Client Analysis: Understanding Request Patterns and Response Times",
                styles=SUB_SECTION_STYLE
            ),
            pn.pane.Markdown(
                "Now let's examine the client-side perspective to understand how requests are being initiated and how long clients are waiting for responses. This analysis will help you identify which RPCs are taking the longest from the client's viewpoint, understand request patterns across your client processes, and spot potential issues with network latency or client-side bottlenecks.",
                styles=DESCRIPTION_STYLE
            ), 
            # Timing Breakdown Section (client-side)
            pn.pane.Markdown("### Timing Breakdown", styles=SUB_SECTION_STYLE),
            pn.pane.Markdown(
                "Now let's switch to the client's perspective. Thicker chords mean the client spent more time on their requests.",
                styles=DESCRIPTION_STYLE
            ), 
            pn.Column(chord_graph_client_view, width=700, height=700),
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
                    "**What this shows:** These are the top 5 client-side RPCs with the highest average call time. For each, you can see the max, average, and min times. Use this to spot slow client operations or network delays.",
                    styles=DESCRIPTION_STYLE
                ),
            ),
            # Workload Distribution (client-side)
            pn.pane.Markdown(
                "How is the number of calls distributed across your processes? This helps you understand load balancing and identify potential bottlenecks.",
                styles=DESCRIPTION_STYLE
            ),
            pn.pane.Markdown("### Who's Sending the Most Requests?", styles=SUB_SECTION_STYLE),
            pn.pane.Markdown(
                "This heatmap shows which client processes are making the most RPC calls.",
                styles=DESCRIPTION_STYLE
            ),
            client_heatmap,
            pn.pane.Markdown(
                "**What this shows:** This heatmap lets you quickly spot which client processes are making the most RPC calls and which types of RPCs they use most often. Use this to find your busiest clients and see if the load is balanced.",
                styles=DESCRIPTION_STYLE
            ),
            # Remote Procedure Call (client-side)
            pn.pane.Markdown("### Understanding the RPC Lifecycle", styles=SUB_SECTION_STYLE),
            pn.pane.Markdown(
                "RPCs go through several stages from initiation to completion. The diagram below shows the typical flow of the server RPC lifecycle:",
                styles=DESCRIPTION_STYLE
            ),
            pn.Row(
                pn.Column(
                    pn.pane.Markdown(
                        "**Client Side:** Initiates the RPC call and waits for response",
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
            ),
            pn.pane.Markdown("### Your RPCs in Detail", styles=SUB_SECTION_STYLE),
            pn.pane.Markdown(
                "Now let's dive even deeper and see the steps for all the selected RPCs. These visualizations show the scaled timing of each step in your selected RPCs, helping you identify exactly where bottlenecks occur in your workflow.",
                styles=DESCRIPTION_STYLE
            ),
            pn.pane.Markdown(
                "This visualization shows the timing breakdown of client-side steps for your selected RPCs. The segments show how the average time was spent in each phase of the client-side process.",
                styles=DESCRIPTION_STYLE
            ),
            pn.Row(
                pn.pane.Markdown(
                    """
                    Do you notice anything unusual? Watch for long set_input or get_output times- this means heavy data serialization, so consider bulk transfers. If iforward_wait is long but starts right after forwarding ends, it's likely a blocking call- try overlapping computation.
                    """,
                    width=400,
                    styles=DESCRIPTION_STYLE
                ),
                svg_origin,
            ),
            pn.pane.Markdown("### Where is Time Being Spent?", styles=SUB_SECTION_STYLE),
            pn.pane.Markdown(
                "This breakdown shows exactly where time is being consumed in your RPC workflow. Look for the tallest bars - those are your bottlenecks!",
                styles=DESCRIPTION_STYLE
            ),
            graph_8_view,
            pn.pane.Markdown(
                "**What this shows:** See how much total time is spent in each step of the RPC process, across all selected RPCs. Taller bars mean more time spent in that step. Focus on the tallest bars to find bottlenecks.",
                styles=DESCRIPTION_STYLE
            ),
            pn.pane.Markdown("### Performance Variability", styles=SUB_SECTION_STYLE),
            pn.pane.Markdown(
                "This chart show which functions have the most unpredictable performance. "
                "High variability (large error bars) might indicate issues that need attention.",
                styles=DESCRIPTION_STYLE
            ),
            graph_10_view,
            pn.pane.Markdown(
                "**Analysis Tips:** Look for patterns in the data. Are certain steps consistently slow? "
                "Do some RPCs show much more variability than others? These insights can guide your optimization efforts.",
                styles=TIP_STYLE
            ),
        ]