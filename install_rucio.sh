#!/bin/sh
# Install rucio clients in virtualenv
#
/usr/bin/virtualenv /opt/rucio
source /opt/rucio/bin/activate
pip install -U pip
pip install rucio-clients
/usr/bin/cat > /opt/rucio/etc/rucio.cfg << EOF
[common]

[client]
rucio_host = https://voatlasrucio-server-prod.cern.ch:443
auth_host = https://voatlasrucio-auth-prod.cern.ch:443
ca_cert =
client_cert = \$X509_USER_PROXY
client_key = \$X509_USER_PROXY
client_x509_proxy = \$X509_USER_PROXY
request_retries = 3
auth_type = x509

[policy]
permission = atlas
schema = atlas
lfn2pfn_algorithm_default = hash
support = hn-atlas-dist-analysis-help@cern.ch
support_rucio = https://github.com/rucio/rucio/issues/
EOF
# To avoid installing gfal2 when you don't need it
/usr/bin/touch /opt/rucio/lib/python2.7/site-packages/gfal2.py

