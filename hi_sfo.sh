#!/bin/bash
#
# Scrape SFO occupancy from Weiner's page and make an rrd plot
#

PNG=/afs/cern.ch/user/d/dcameron/www/dev/hi_2018/sfo-usage.png
RRDFILE=/home/dcameron/dev/ddm/hicharts/sfo.rrd

# Get total occupancy from web page
usageGB=`curl -s https://vandelli.web.cern.ch/vandelli/sfosummary.html | grep "There are currently .*" | sed -e 's/There are currently .* files (\(.*\) GB) waiting for deletion on the SFO disks./\1/g'`
usage=`echo "$usageGB * 1000 * 1000 * 1000" | bc`

# Add to rrd
rrdtool update $RRDFILE "N:$usage"

# Make latest plot
# 1542034800 = 2018-11-12 16:00:00
rrdtool graph "$PNG" \
  --start 1542034800 \
  --title "SFO usage" \
  --vertical-label "bytes" \
  --width 600 \
  --height 300 \
  --lower-limit 0 \
  --no-legend \
  --font 'DEFAULT:10:Nimbus Sans L' \
  "DEF:sfoused=$RRDFILE:sfoused:AVERAGE" \
  AREA:sfoused#009900:SFOused > /dev/null

