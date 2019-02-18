#
# Plot a histogram of dataset sizes by access time. The data is read from files
# which consist of lines of dataset size, access time and can be generated
# from Rucio dumps, for example like this for RSE type=GROUPDISK
# rucio list-rses --expression "type=GROUPDISK" | while read RSE; do wget https://rucio-hadoop.cern.ch/consistency_datasets?rse=$RSE -O - | awk -F "\t" '{printf $5 " "; if ($7>0) {print $7} else {print $6}}' > rses/$RSE; done
#
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# read the dates and sizes from rse files
data = []
weights = []
rses = []
for rse in os.listdir('rses'):
    rses.append(rse)
    d = []
    w = []
    with open(os.path.join('rses', rse)) as f:
        for l in f:
            (s, t) = l.split()
            d.append(float(t)/1000)
            w.append(float(s)/1000/1000/1000/1000)
        # convert the epoch format to matplotlib date format 
        mpl_data = mdates.epoch2num(d)
        data.append(mpl_data)
        weights.append(w)

# plot it
fig, ax = plt.subplots(1,1)
ax.hist(data, bins=50, stacked=True, weights=weights, label=rses)
ax.xaxis.set_major_locator(mdates.YearLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m.%y'))
plt.ylabel('TB')
plt.title('Dataset size vs last accessed time per GROUPDISK')
#plt.legend(loc=2, prop={'size': 6})
plt.show()

