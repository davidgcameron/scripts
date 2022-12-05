# Print non-pledged sites wallclock hours and efficency to check for
# inclusion in computing acknowledgements

import sys
import pandas as pd

csv = sys.argv[1]

df = pd.read_csv(sys.argv[1])
df['mwc'] = df['wc_in_hours'] / 10**6

print("All sites")
print(df[df.tier == 3][['site', 'mwc', 'ratio']].groupby('site').agg({'mwc': 'sum', 'ratio': 'mean'}))

wclimit = 5
rlimit = 40

print()
print(f"Sites above limit of {wclimit}M wallclock hours and {rlimit}% efficiency")
print(df[(df.tier == 3) & (df.mwc > wclimit) & (df.ratio > rlimit)][['site', 'mwc', 'ratio']].groupby('site').agg({'mwc': 'sum', 'ratio': 'mean'}))

