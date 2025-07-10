import glob
from mochi_perf.statistics import MochiStatistics
from dashboard_components import MochiDashboard

# Change to your mochi logs  
PATH = './data/*.stats.json'

if __name__ == '__main__':
    stats = MochiStatistics()       

    for file_path in glob.glob(PATH):
        stats.add_file(file_path)

    MochiDashboard(stats)   