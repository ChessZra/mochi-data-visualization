# Objective
This project aims to analyze and visualize a set of json files produced by margo (Mochi).

We want a project that is interactive and also able to produce static pdfs...

Useful library ideas: networkx, panel, ...

# Visualization Ideas
Latency Distribution Histograms (currently implementing)
    - Show min/max/avg latency for different RPC types
    - Highlight outliers that exceed certain thresholds

RPC Call Graph Visualization
    - Create a directed graph showing RPC call chains (parent-child relationships)
    - Size nodes based on execution time or frequency
    - Color code by RPC type

Timing Breakdown Dashboard
    - Stacked bar charts showing the various timing components of RPCs:
        - Handler duration
        - ULT (user-level thread) duration
        - Forward/respond times
        - Bulk data transfer times

Server Load Balancing View
    - Heatmap showing traffic distribution across servers
    - Metrics could include: request count, data volume, processing time

Bulk Data Transfer Analysis
    - Visualize data transfer sizes vs. duration
    - Calculate and display effective bandwidth