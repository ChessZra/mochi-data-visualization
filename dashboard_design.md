# Mochi Performance Analysis Dashboard - Visual Design

## Overall Layout

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    🍡 Mochi Performance Analysis Dashboard 🍡                │
├─────────────────────────────────────────────────────────────────────────────┤
│ [📁 Upload] [📊 Export] [⚙️ Settings]                    [🏠] [🔍] [📈] │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                           SECTION 1: PROCESS OVERVIEW                  │ │
│  ├─────────────────────────────────────────────────────────────────────────┤ │
│  │                                                                         │ │
│  │  ┌─────────────────────────────────────────────────────────────────────┐ │ │
│  │  │  📊 Total RPC Execution Time by Process                             │ │ │
│  │  │                                                                     │ │ │
│  │  │  ████ Process 0      ████████ Process 1  ████████████ Process 2     │ │ │
│  │  │  ████ Process 3      ████████ Process 4                             │ │ │
│  │  └─────────────────────────────────────────────────────────────────────┘ │ │
│  │                                                                         │ │
│  │  ┌─────────────────────────────────────────────────────────────────────┐ │ │
│  │  │  📝 Description: This chart shows the cumulative time each process  │ │ │
│  │  │  spent executing RPC requests. The height of each bar represents    │ │ │
│  │  │  the total execution time for that process, helping you identify    │ │ │
│  │  │  which processes are doing the most computational work.             │ │ │
│  │  └─────────────────────────────────────────────────────────────────────┘ │ │
│  │                                                                         │ │
│  │  ┌─────────────────────────────────────────────────────────────────────┐ │ │
│  │  │  📊 Total RPC Call Time by Process                                  │ │ │
│  │  │                                                                     │ │ │
│  │  │  ████ Process 0      ████████ Process 1  ████████████ Process 2     │ │ │
│  │  │  ████ Process 3      ████████ Process 4                             │ │ │
│  │  └─────────────────────────────────────────────────────────────────────┘ │ │
│  │                                                                         │ │
│  │  ┌─────────────────────────────────────────────────────────────────────┐ │ │
│  │  │  📝 Description: This chart displays the total time each process    │ │ │
│  │  │  spent making RPC calls to other processes. It shows the client-    │ │ │
│  │  │  side perspective of RPC communication, revealing which processes   │ │ │
│  │  │  are the most active clients.                                       │ │ │
│  │  └─────────────────────────────────────────────────────────────────────┘ │ │
│  │                                                                         │ │
│  │  ┌─────────────────────────────────────────────────────────────────────┐ │ │
│  │  │                    Summary Statistics                                │ │ │
│  │  │  Total RPCs: 4,962  │  Avg Time: 3.2ms  │  Max Time: 128ms     │ │ │
│  │  └─────────────────────────────────────────────────────────────────────┘ │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                        SECTION 2: PROCESS DEEP DIVE                    │ │
│  ├─────────────────────────────────────────────────────────────────────────┤ │
│  │                                                                         │ │
│  │  Process: [Process 2 ▼]  Time Range: [Last Hour ▼]  Filter: [All RPCs ▼] │ │
│  │                                                                         │ │
│  │  ┌─────────────────────────────────────────────────────────────────────┐ │ │
│  │  │  📊 RPC Call Times for Selected Process                             │ │ │
│  │  │                                                                     │ │ │
│  │  │  ████████████ RPC_A  ████████ RPC_B  ████ RPC_C  ██ RPC_D          │ │ │
│  │  └─────────────────────────────────────────────────────────────────────┘ │ │
│  │                                                                         │ │
│  │  ┌─────────────────────────────────────────────────────────────────────┐ │ │
│  │  │  📝 Description: This detailed view shows how much time the selected│ │ │
│  │  │  process spent calling each specific RPC type. It helps you         │ │ │
│  │  │  understand the client-side behavior of a particular process.       │ │ │
│  │  └─────────────────────────────────────────────────────────────────────┘ │ │
│  │                                                                         │ │
│  │  ┌─────────────────────────────────────────────────────────────────────┐ │ │
│  │  │  📊 RPC Execution Times for Selected Process                        │ │ │
│  │  │                                                                     │ │ │
│  │  │  ████████████ RPC_A  ████████ RPC_B  ████ RPC_C  ██ RPC_D          │ │ │
│  │  └─────────────────────────────────────────────────────────────────────┘ │ │
│  │                                                                         │ │
│  │  ┌─────────────────────────────────────────────────────────────────────┐ │ │
│  │  │  📝 Description: This chart shows how much time the selected process│ │ │
│  │  │  spent executing each type of RPC request it received. It reveals  │ │ │
│  │  │  the server-side workload distribution.                            │ │ │
│  │  └─────────────────────────────────────────────────────────────────────┘ │ │
│  │                                                                         │ │
│  │  ┌─────────────────────────────────────────────────────────────────────┐ │ │
│  │  │  Process Statistics: Total Calls: 1,245 | Avg Call Time: 2.1ms      │ │ │
│  │  │  ⚠️ Alert: High serialization time detected in RPC_A                │ │ │
│  │  └─────────────────────────────────────────────────────────────────────┘ │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐ │
│  │                          SECTION 3: RPC ANALYSIS                       │ │
│  ├─────────────────────────────────────────────────────────────────────────┤ │
│  │                                                                         │ │
│  │  ┌─────────────────────────────────────────────────────────────────────┐ │ │
│  │  │  Controls: [Metric: RPC Time ▼] [Aggregation: Total ▼]              │ │ │
│  │  │            [Group By: RPC Type ▼] [View: Bar Chart ▼]               │ │ │
│  │  └─────────────────────────────────────────────────────────────────────┘ │ │
│  │                                                                         │ │
│  │  ┌─────────────────────────────────────────────────────────────────────┐ │ │
│  │  │  📊 Main Visualization (Dynamic)                                    │ │ │
│  │  │                                                                     │ │ │
│  │  │  ████████████ margo_rpc_handler (2,453)                            │ │ │
│  │  │  ████████ bulk_transfer (1,245)                                     │ │ │
│  │  │  ██████ custom_rpc_type (843)                                       │ │ │
│  │  │  ███ other_rpc (421)                                                │ │ │
│  │  └─────────────────────────────────────────────────────────────────────┘ │ │
│  │                                                                         │ │
│  │  ┌─────────────────────────────────────────────────────────────────────┐ │ │
│  │  │  📝 Description: This chart adapts based on your metric, aggregation│ │ │
│  │  │  and grouping selections. It provides flexible analysis capabilities│ │ │
│  │  │  for exploring different aspects of your RPC performance data.     │ │ │
│  │  └─────────────────────────────────────────────────────────────────────┘ │ │
│  │                                                                         │ │
│  │  ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────┐ │ │
│  │  │   Summary Stats     │  │   Distribution      │  │   Diagnostics   │ │ │
│  │  │                     │  │   Heatmap           │  │                 │ │ │
│  │  │  Total: 4,962       │  │                     │  │  ⚠️ Long        │ │ │
│  │  │  Avg: 3.2ms         │  │  Source → Dest      │  │  serialization  │ │ │
│  │  │  Max: 128ms         │  │  communication      │  │  time in        │ │ │
│  │  │  Data: 2.8GB        │  │  patterns           │  │  bulk_transfer  │ │ │
│  │  └─────────────────────┘  └─────────────────────┘  └─────────────────┘ │ │
│  │                                                                         │ │
│  │  ┌─────────────────────────────────────────────────────────────────────┐ │ │
│  │  │  📝 Description: The heatmap visualizes communication patterns      │ │ │
│  │  │  between processes. The diagnostics panel automatically analyzes    │ │ │
│  │  │  your data and highlights potential optimization opportunities.     │ │ │
│  │  └─────────────────────────────────────────────────────────────────────┘ │ │
│  └─────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Interactive Elements

### Navigation Flow
```
Section 1 (Overview) → Click on Process Bar → Section 2 (Deep Dive)
Section 2 (Deep Dive) → Select Process → Detailed Analysis
Section 3 (RPC Analysis) → Dynamic based on selections
```

### Color Coding
- **Blue**: Normal performance ranges
- **Orange**: Moderate performance issues
- **Red**: Critical performance issues
- **Green**: Optimal performance

### Hover Interactions
- **Bar Charts**: Show exact values, percentages, and details
- **Heatmap**: Show source/destination pair details
- **Process Bars**: Show process rank, total calls, average times

### Responsive Design
- **Desktop**: Full layout with all sections visible
- **Tablet**: Stacked layout with collapsible sections
- **Mobile**: Single column layout with tab navigation

## Key Features

### Section 1: Process Overview
- **Purpose**: System-wide performance snapshot
- **Interactions**: Click bars to drill down to specific processes
- **Updates**: Real-time as data loads

### Section 2: Process Deep Dive
- **Purpose**: Detailed process analysis
- **Interactions**: Process selection, time filtering, RPC filtering
- **Features**: Performance alerts, comparison mode

### Section 3: RPC Analysis
- **Purpose**: Flexible RPC-level analysis
- **Interactions**: Multiple dropdown controls, dynamic visualizations
- **Features**: Export capabilities, custom metrics

## Graph Explanations

### Section 1: Process Overview

#### **Total RPC Execution Time by Process**
This chart shows the cumulative time each process spent executing RPC requests. The height of each bar represents the total execution time for that process, helping you identify which processes are doing the most computational work. Processes with longer bars are either handling more requests or taking longer to process each request. This visualization is crucial for identifying computational bottlenecks and understanding load distribution across your system.

#### **Total RPC Call Time by Process**
This chart displays the total time each process spent making RPC calls to other processes. It shows the client-side perspective of RPC communication, revealing which processes are the most active clients. High values indicate processes that are heavily dependent on remote services, while low values might suggest processes that are more self-contained or primarily serving requests rather than making them.

#### **Summary Statistics Panel**
This panel provides key system-wide metrics at a glance. The total RPC count gives you the overall communication volume, while average and maximum execution times help you understand typical performance and identify outliers. These metrics serve as quick health indicators for your distributed system.

### Section 2: Process Deep Dive

#### **RPC Call Times for Selected Process**
This detailed view shows how much time the selected process spent calling each specific RPC type. It helps you understand the client-side behavior of a particular process, revealing which remote services it depends on most heavily. This information is valuable for optimizing client-side code and understanding service dependencies.

#### **RPC Execution Times for Selected Process**
This chart shows how much time the selected process spent executing each type of RPC request it received. It reveals the server-side workload distribution, helping you identify which RPC types are most computationally expensive for this process. This insight is crucial for optimizing server-side performance and resource allocation.

#### **Process Statistics Panel**
This panel provides detailed metrics specific to the selected process, including total call counts and average response times. The performance alerts highlight potential issues like high serialization times or blocking calls, providing actionable insights for optimization.

### Section 3: RPC Analysis

#### **Main Visualization (Dynamic)**
This chart adapts based on your metric, aggregation, and grouping selections. It provides flexible analysis capabilities, allowing you to explore different aspects of your RPC performance data. Whether you're looking at total execution times by RPC type, average call times by source process, or any other combination, this visualization helps you discover patterns and identify optimization opportunities.

#### **Distribution Heatmap**
This heatmap visualizes communication patterns between processes, with source processes on one axis and destination processes on the other. The color intensity represents the volume of communication (RPC count, data transferred, or time spent). This visualization is essential for understanding your system's communication topology and identifying potential bottlenecks in inter-process communication.

#### **Diagnostics Panel**
This panel automatically analyzes your performance data and highlights potential issues that warrant attention. It provides specific recommendations for optimization, such as suggesting bulk transfers for high serialization times or identifying opportunities for parallel execution. These insights help you prioritize performance improvements.

This design provides a comprehensive workflow from high-level system overview to detailed RPC analysis, with intuitive navigation and rich interactive capabilities. 