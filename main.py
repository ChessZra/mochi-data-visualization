import glob
from mochi_perf.statistics import MochiStatistics
from dashboard_components import MochiDashboard
  
PATH = './data-1/*.stats.json'

if __name__ == '__main__':
    stats = MochiStatistics()       

    # load all the folders for the sake of demonstration
    for file_path in glob.glob('./data-5/*.stats.json') + glob.glob('./data-2/*.stats.json') + glob.glob('./data-1/*.stats.json'):
        stats.add_file(file_path)

    MochiDashboard(stats)   