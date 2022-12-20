# Print non-pledged sites wallclock hours and efficency to check for
# inclusion in computing acknowledgements

import sys
import pandas as pd

# Minimum thresholds for being acknowledged in M wallclock hours and % efficiency
wclimit = 5
rlimit = 40

if len(sys.argv) != 2:
    print('Usage: python3 ack.py <comuptingsites.csv file>')
    sys.exit(0)

csv = sys.argv[1]

# Read in the computingsites.csv file
df = pd.read_csv(sys.argv[1])

# Get cpu and walltime for T3 sites
aggdf = df[df.tier == 3][['site', 'sum_walltime', 'sum_cpuconsumptiontime']].groupby('site').agg({'sum_walltime': 'sum', 'sum_cpuconsumptiontime': 'sum'})
# Efficiency
aggdf['eff'] = aggdf['sum_cpuconsumptiontime'] / aggdf['sum_walltime'] * 100
# Million wallclock hours
aggdf['mwc'] = aggdf['sum_walltime'] / 3600 / 10**6

print("All sites")
print(aggdf)
print(aggdf[['mwc', 'eff']])

print()
print(f"Sites above limit of {wclimit}M wallclock hours and {rlimit}% efficiency")
print(aggdf[(aggdf.mwc > wclimit) & (aggdf.eff > rlimit)][['mwc', 'eff']])

