import sys
import pandas as pd

central_services = 89.85
csv = sys.argv[1]
fraction_of_year = float(sys.argv[2]) if len(sys.argv) > 2 else 1
resource_type = sys.argv[3] if len(sys.argv) > 3 else None

df = pd.read_csv(sys.argv[1])
df['khs06y'] = df['sum_hs06sec']/86400/365/1000/fraction_of_year
df['eff'] = df['sum_cpuconsumptiontime']/df['sum_walltime']

print('Vega')
dfv = df[df.site == 'Vega'][['sum_cpuconsumptiontime', 'sum_walltime', 'khs06y']]
print(dfv.sum())

print('Sim@P1')

dfp1 = df[df.site == 'CERN-P1'][['sum_cpuconsumptiontime', 'sum_walltime', 'khs06y']]
dfp1['eff'] = dfp1['sum_cpuconsumptiontime']/dfp1['sum_walltime']

print(dfp1.sum())
print(f"Overall efficiency {dfp1['sum_cpuconsumptiontime'].sum()/dfp1['sum_walltime'].sum()}")
print()
print('Resource types')

# Add Vega to HPC_special
df.loc[df['site'] == 'Vega', 'resourcetype'] = 'hpc_special'

dfr = df[['sum_walltime', 'sum_cpuconsumptiontime', 'khs06y', 'resourcetype']].groupby('resourcetype').sum()
dfr['eff'] = dfr['sum_cpuconsumptiontime']/dfr['sum_walltime']

print(dfr)

# Remove Vega from T2s
df.loc[df['site'] == 'Vega', 'tier'] = -1
dft = df[df.resourcetype.isin(['GRID', 'cloud', 'hpc'])][['sum_walltime', 'sum_cpuconsumptiontime', 'khs06y', 'tier']].groupby('tier').sum()
dft['eff'] = dft['sum_cpuconsumptiontime']/dft['sum_walltime']

print(dft)


