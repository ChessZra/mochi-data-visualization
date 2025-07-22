import glob
from mochi_perf.statistics import MochiStatistics
from dashboard_components import MochiDashboard

# Change to your mochi logs  
PATH = './bulk-data/*.stats.json'

if __name__ == '__main__':    
    stats = MochiStatistics(enable_profiling=True)       

    print("Processing files with profiling enabled...")
    for file_path in glob.glob(PATH):
        stats.add_file(file_path)

    print(stats.get_profiling_report())
    MochiDashboard(stats)   