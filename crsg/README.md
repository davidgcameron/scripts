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

Get data from grafana
Note that the dates are inclusive

`python3 crsg.py -f 2021-01-01 -t 2021-12-31 -o results-2021`

### C-RSG reports

The script prints a summary by tier and resource type.

Run the summary script

`python3 summary.py results-2021/computingsites.csv`

Get normalised kHS06y numbers for partial years, eg if using three months:

`python3 summary.py results-2021/computingsites.csv 0.25`

Script currently excludes Vega from T2s and adds it to HPC_special

T0, T1 and T2 numbers are from resource types GRID, cloud and hpc

For T0 the central services numbers should be added (89 kHS06 in 2021)

### Computing acknowledgements

This script prints wallclock time and efficiency for unpledged sites.

`python3 ack.py results-2021/computingsites.csv`

The limits for wallclock and efficiency are set in ack.py

