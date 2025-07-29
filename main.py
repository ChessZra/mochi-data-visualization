import glob
from mochi_perf.statistics import MochiStatistics
from dashboard_components import MochiDashboard
from dask.distributed import Client

client = Client()  # use all cores
num_cores = sum(client.nthreads().values())
print(f'Using {num_cores} cores')

# Change to your mochi logs  
PATH = './clean-data/*.stats.json'

if __name__ == '__main__':    
    files = glob.glob(PATH)
    stats = MochiStatistics(files=files, num_cores=num_cores)            
    print(stats.get_profiling_report())
    MochiDashboard(stats)   