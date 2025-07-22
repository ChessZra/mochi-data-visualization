# Mochi Data Visualization

A Python-based interactive dashboard for visualizing and analyzing Mochi HPC performance statistics. This tool provides comprehensive insights into RPC execution times, client call times, bulk transfer performance, and other key metrics from Mochi performance data.
## Installation
### Step 1: Clone the Repository

```bash
git clone <repository-url>
cd mochi-data-visualization
```

### Step 2: Create a Virtual Environment (Recommended)

```bash
# Create virtual environment
python -m venv .venv

# Activate virtual environment
# On Windows:
.venv\Scripts\activate
# On macOS/Linux:
source .venv/bin/activate
```

### Step 3: Install Dependencies

```bash
pip install panel holoviews hvplot pandas pycairo ipython psutil
```

## Usage

### Quick Start
```bash
python main.py
```