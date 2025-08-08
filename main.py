import glob
import multiprocessing as mp

from mochi_perf.statistics import MochiStatistics
from dashboard_components import MochiDashboard

# Change to your mochi logs  
PATH = './data/quintain/*.stats.json'

if __name__ == '__main__':    
    num_cores = mp.cpu_count()
    print(f'Using {num_cores} cores for processing')
    files = glob.glob(PATH)
    stats = MochiStatistics(files=files, num_cores=num_cores)
    MochiDashboard(stats)