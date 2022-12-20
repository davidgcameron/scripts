## Accounting summary scripts

Tools to generate summary numbers for C-RSG reports and computing acknowledgements.
They pull info from Grafana and manipulate the data with pandas.

### Installation

Set up env:

```
python3 -mvenv crsg-env
source crsg-env/bin/activate
pip install pandas requests
```

Edit crsg.py to add the Grafana access token, which can be found at https://gitlab.cern.ch/atlas-adc-monitoring/grafana-api/-/blob/master/Examples/CRSG%20report/Scripts/crsg.py

Get data from grafana for the desired time period - note that the dates are inclusive

`python3 crsg.py -f 2021-01-01 -t 2021-12-31 -o results-2021`

### C-RSG reports

The script prints a summary by tier and resource type.

Run the summary script on the `computingsites.csv` file in the results directory:

`python3 summary.py results-2021/computingsites.csv`

Get normalised kHS06y numbers for partial years, eg if using three months:

`python3 summary.py results-2021/computingsites.csv 0.25`

The script currently excludes Vega from T2s and adds it to HPC_special

T0, T1 and T2 numbers are from resource types GRID, cloud, Tier-0 and hpc

For T0 the central services numbers should be added (89 kHS06 in 2021)

### Computing acknowledgements

This script prints wallclock time (million wallclock hours) and efficiency (cpu time / wall time) for unpledged sites.

`python3 ack.py results-2021/computingsites.csv`

It also prints a table of sites over the limits for wallclock and efficiency, which are set in ack.py

