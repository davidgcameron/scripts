# Query AGIS for ARC CEs and check their batch system from local LDAP infosys
#
import json
import requests
import subprocess
import re
from urlparse import urlparse

# Get AGIS info
j = requests.get('http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&vo_name=atlas')
data = j.json()

# Get ARC CE hosts
qs = []
for q, a in data.items():
    qs.extend(a['queues'])

qs = [q for q in qs if q['ce_flavour'] == 'ARC-CE']
endpoints = [q['ce_endpoint'] for q in qs]
ep = set(endpoints)
hosts = []
for e in ep:
    if e.find('://') != -1:
        hosts.append(urlparse(e).netloc.split(':')[0])
    else:
        hosts.append(e.split(':')[0])

# Query LDAP
for h in hosts:
    print h,
    args = ("ldapsearch -LLL -x -h %s -p 2135 -b mds-vo-name=local,o=grid nordugrid-cluster-name=%s nordugrid-cluster-lrms-type" % (h, h)).split()
    try:
        op = subprocess.check_output(args)
    except subprocess.CalledProcessError:
        print 'unknown'
        continue
    m = re.search(r'nordugrid-cluster-lrms-type: (\w*)', op)
    if m is None:
        print 'unknown'
    else:
        print m.groups()[0]


