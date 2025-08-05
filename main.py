import glob

from mochi_perf.statistics import MochiStatistics
from dashboard_components import MochiDashboard
from dask.distributed import Client

# Change to your mochi logs  
PATH = './data/sample/*.stats.json'

if __name__ == '__main__':    
    client = Client()  # use all cores
    num_workers = len(client.nthreads())
    num_threads = sum(client.nthreads().values())
    print(f'Using {num_workers} workers with {num_threads} total threads')
    files = glob.glob(PATH)
    stats = MochiStatistics(files=files, num_cores=num_threads)            
    MochiDashboard(stats)